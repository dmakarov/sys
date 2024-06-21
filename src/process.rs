use {
    crate::{
        db::*,
        exchange::*,
        notifier::*,
        priority_fee::{apply_priority_fee, PriorityFee},
        token::*,
        *,
    },
    chrono::prelude::*,
    console::style,
    itertools::{izip, Itertools},
    rpc_client_utils::{get_signature_date, get_stake_activation_state, StakeActivationState},
    rust_decimal::prelude::*,
    separator::FixedPlaceSeparatable,
    solana_client::rpc_config::RpcTransactionConfig,
    solana_sdk::{
        compute_budget,
        message::Message,
        native_token::Sol,
        pubkey::Pubkey,
        signature::{read_keypair_file, Keypair, Signature, Signer},
        signers::Signers,
        system_instruction, system_program,
        transaction::Transaction,
    },
    std::{
        collections::{BTreeMap, HashSet},
        io::Write,
    },
};

pub fn today() -> NaiveDate {
    let today = Local::now().date_naive();
    NaiveDate::from_ymd_opt(today.year(), today.month(), today.day()).unwrap()
}

pub fn is_long_term_cap_gain(acquisition: NaiveDate, disposal: Option<NaiveDate>) -> bool {
    let disposal = disposal.unwrap_or_else(today);
    let hold_time = disposal - acquisition;
    hold_time >= chrono::Duration::try_days(365).unwrap()
}

pub fn format_order_side(order_side: OrderSide) -> String {
    match order_side {
        OrderSide::Buy => style(" Buy").green(),
        OrderSide::Sell => style("Sell").red(),
    }
    .to_string()
}

async fn get_block_date_and_price(
    rpc_client: &RpcClient,
    slot: Slot,
    token: MaybeToken,
) -> Result<(NaiveDate, Decimal), Box<dyn std::error::Error>> {
    let block_date = rpc_client_utils::get_block_date(rpc_client, slot).await?;
    Ok((
        block_date,
        retry_get_historical_price(rpc_client, block_date, token).await?,
    ))
}

async fn retry_get_historical_price(
    rpc_client: &RpcClient,
    block_date: NaiveDate,
    token: MaybeToken,
) -> Result<Decimal, Box<dyn std::error::Error>> {
    const NUM_RETRIES: usize = 20;
    for _ in 1..NUM_RETRIES {
        let price = token.get_historical_price(rpc_client, block_date).await;
        if price.is_ok() {
            return price;
        }
        // Empirically observed cool down period is ~14s
        //
        // TODO: Move this retry logic into `coin_gecko::get_historical_price()`, and respect the
        // HTTP `Retry-After:` response header from Coin Gecko
        sleep(Duration::from_secs(5));
    }
    token.get_historical_price(rpc_client, block_date).await
}

fn println_jup_quote<W: Write>(
    from_token: MaybeToken,
    to_token: MaybeToken,
    quote: &jup_ag::Quote,
    writer: &mut W,
) {
    let route = quote
        .route_plan
        .iter()
        .map(|route_plan| route_plan.swap_info.label.clone().unwrap_or_default())
        .join(", ");
    writeln!(
        writer,
        "Swap {}{} for {}{} (min: {}{}) via {}",
        from_token.symbol(),
        from_token.ui_amount(quote.in_amount),
        to_token.symbol(),
        to_token.ui_amount(quote.out_amount),
        to_token.symbol(),
        to_token.ui_amount(quote.other_amount_threshold),
        route,
    )
    .unwrap();
}

pub async fn process_jup_quote<W: Write>(
    from_token: MaybeToken,
    to_token: MaybeToken,
    ui_amount: f64,
    slippage_bps: u64,
    writer: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    let quote = jup_ag::quote(
        from_token.mint(),
        to_token.mint(),
        from_token.amount(ui_amount),
        jup_ag::QuoteConfig {
            slippage_bps: Some(slippage_bps),
            ..jup_ag::QuoteConfig::default()
        },
    )
    .await?;

    println_jup_quote(from_token, to_token, &quote, writer);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn process_jup_swap<T: Signers, W: Write>(
    db: &mut Db,
    rpc_clients: &RpcClients,
    address: Pubkey,
    from_token: MaybeToken,
    to_token: MaybeToken,
    ui_amount: Option<f64>,
    slippage_bps: u64,
    lot_selection_method: LotSelectionMethod,
    lot_numbers: Option<HashSet<usize>>,
    signers: T,
    existing_signature: Option<Signature>,
    if_from_balance_exceeds: Option<u64>,
    for_no_less_than: Option<f64>,
    max_coingecko_value_percentage_loss: f64,
    priority_fee: PriorityFee,
    notifier: &Notifier,
    writer: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_client = rpc_clients.default();

    let from_account = db
        .get_account(address, from_token)
        .ok_or_else(|| format!("{from_token} account does not exist for {address}"))?;

    let from_token_price = from_token.get_current_price(rpc_client).await?;
    let to_token_price = to_token.get_current_price(rpc_client).await?;

    if let Some(existing_signature) = existing_signature {
        db.record_swap(
            existing_signature,
            0, /*last_valid_block_height*/
            address,
            from_token,
            from_token_price,
            to_token,
            to_token_price,
            lot_selection_method,
            lot_numbers,
        )?;
    } else {
        let amount = match ui_amount {
            Some(ui_amount) => from_token.amount(ui_amount),
            None => from_account.last_update_balance,
        };

        if from_account.last_update_balance < amount {
            return Err(format!(
                "Insufficient {} balance in {}. Tracked balance is {}",
                from_token,
                address,
                from_token.ui_amount(from_account.last_update_balance)
            )
            .into());
        }

        let swap_prefix = format!("Swap {from_token}->{to_token}");

        if let Some(if_from_balance_exceeds) = if_from_balance_exceeds {
            if from_account.last_update_balance < if_from_balance_exceeds {
                writeln!(
                    writer,
                    "{swap_prefix} declined because {} ({}) balance is less than {}{}",
                    address,
                    from_token.name(),
                    from_token.symbol(),
                    from_token.ui_amount(if_from_balance_exceeds)
                )?;
                return Ok(());
            }
        }

        let _ = to_token.balance(rpc_client, &address).map_err(|err| {
            format!(
                "{} account does not exist for {}. \
                To create it, run `spl-token create-account {} --owner {}: {}",
                to_token,
                address,
                to_token.mint(),
                address,
                err
            )
        })?;

        writeln!(writer, "Fetching best {from_token}->{to_token} quote...")?;
        let quote = jup_ag::quote(
            from_token.mint(),
            to_token.mint(),
            amount,
            jup_ag::QuoteConfig {
                slippage_bps: Some(slippage_bps),
                ..jup_ag::QuoteConfig::default()
            },
        )
        .await?;

        println_jup_quote(from_token, to_token, &quote, writer);

        let from_value =
            from_token_price * Decimal::from_f64(from_token.ui_amount(quote.in_amount)).unwrap();
        let min_to_value = to_token_price
            * Decimal::from_f64(to_token.ui_amount(quote.other_amount_threshold)).unwrap();

        let swap_value_percentage_loss = Decimal::from_usize(100).unwrap()
            - min_to_value / from_value * Decimal::from_usize(100).unwrap();

        writeln!(
            writer,
            "Coingecko value loss: {swap_value_percentage_loss:.2}%"
        )?;
        if swap_value_percentage_loss
            > Decimal::from_f64(max_coingecko_value_percentage_loss).unwrap()
        {
            return Err(format!(
                "{swap_prefix} exceeds the max value loss ({max_coingecko_value_percentage_loss:2}%) relative to CoinGecko token price"
            )
            .into());
        }

        if let Some(for_no_less_than) = for_no_less_than {
            let to_token_amount = to_token.ui_amount(quote.other_amount_threshold);

            if to_token_amount < for_no_less_than {
                let to_token_symbol = to_token.symbol();
                let msg = format!("{swap_prefix} would not result in at least {to_token_symbol}{for_no_less_than} tokens, only would have received {to_token_symbol}{to_token_amount}");
                writeln!(writer, "{msg}")?;
                notifier.send(&msg).await;
                return Ok(());
            }
        }

        writeln!(writer, "Generating {swap_prefix} Transaction...")?;
        let mut swap_request = jup_ag::SwapRequest::new(address, quote.clone());
        swap_request.wrap_and_unwrap_sol = Some(from_token.is_sol() || to_token.is_sol());

        if let Some(lamports) = priority_fee.exact_lamports() {
            swap_request.prioritization_fee_lamports =
                jup_ag::PrioritizationFeeLamports::Exact { lamports };
        }

        let mut transaction = jup_ag::swap(swap_request).await?.swap_transaction;

        {
            let mut transaction_compute_budget = priority_fee::ComputeBudget::default();

            let static_account_keys = transaction.message.static_account_keys();
            for instruction in transaction.message.instructions() {
                if let Some(program_id) =
                    static_account_keys.get(instruction.program_id_index as usize)
                {
                    if *program_id == compute_budget::id() {
                        match solana_sdk::borsh1::try_from_slice_unchecked(&instruction.data) {
                            Ok(compute_budget::ComputeBudgetInstruction::SetComputeUnitLimit(
                                compute_unit_limit,
                            )) => {
                                transaction_compute_budget.compute_unit_limit = compute_unit_limit;
                            }
                            Ok(compute_budget::ComputeBudgetInstruction::SetComputeUnitPrice(
                                micro_lamports,
                            )) => {
                                transaction_compute_budget.compute_unit_price_micro_lamports =
                                    micro_lamports;
                            }
                            _ => {}
                        }
                    }
                }
            }
            if transaction_compute_budget.priority_fee_lamports() > priority_fee.max_lamports() {
                return Err(format!(
                    "Swap too expensive. Priority fee of {} is greater than max fee of {}",
                    Sol(transaction_compute_budget.priority_fee_lamports()),
                    Sol(priority_fee.max_lamports())
                )
                .into());
            }
            writeln!(
                writer,
                "Swap priority fee: {}",
                Sol(transaction_compute_budget.priority_fee_lamports())
            )?;
        }

        let (recent_blockhash, last_valid_block_height) =
            rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;
        transaction.message.set_recent_blockhash(recent_blockhash);

        let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
        if simulation_result.err.is_some() {
            return Err(
                format!("Swap transaction simulation failure: {simulation_result:?}").into(),
            );
        }

        assert_eq!(transaction.signatures[0], Signature::default());
        let signatures = signers.try_sign_message(&transaction.message.serialize())?;
        assert_eq!(signatures.len(), 1);
        let signature = signatures[0];
        transaction.signatures[0] = signature;

        if db.get_account(address, to_token).is_none() {
            let epoch = rpc_client.get_epoch_info()?.epoch;
            db.add_account(TrackedAccount {
                address,
                token: to_token,
                description: from_account.description,
                last_update_epoch: epoch,
                last_update_balance: 0,
                lots: vec![],
                no_sync: None,
            })?;
        }
        db.record_swap(
            signature,
            last_valid_block_height,
            address,
            from_token,
            from_token_price,
            to_token,
            to_token_price,
            lot_selection_method,
            lot_numbers,
        )?;

        if !send_transaction_until_expired(rpc_clients, &transaction, last_valid_block_height)
            .unwrap_or_default()
        {
            db.cancel_swap(signature)?;
            return Err("Swap failed".into());
        }
    }
    Ok(())
}

pub async fn process_sync_swaps<W: Write>(
    db: &mut Db,
    rpc_client: &RpcClient,
    notifier: &Notifier,
    writer: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    let block_height = rpc_client.get_epoch_info()?.block_height;

    for PendingSwap {
        signature,
        last_valid_block_height,
        address,
        from_token,
        to_token,
        ..
    } in db.pending_swaps()
    {
        let swap = format!("swap ({address}: {from_token} -> {to_token})");

        let status = rpc_client.get_signature_status_with_commitment_and_history(
            &signature,
            rpc_client.commitment(),
            true,
        )?;
        match status {
            Some(result) => {
                if result.is_ok() {
                    writeln!(writer, "Pending {swap} confirmed: {signature}")?;
                    let result = rpc_client.get_transaction_with_config(
                        &signature,
                        RpcTransactionConfig {
                            commitment: Some(rpc_client.commitment()),
                            max_supported_transaction_version: Some(0),
                            ..RpcTransactionConfig::default()
                        },
                    )?;

                    let block_time = result
                        .block_time
                        .ok_or("Transaction block time not available")?;

                    let when = Local.timestamp_opt(block_time, 0).unwrap();
                    let when =
                        NaiveDate::from_ymd_opt(when.year(), when.month(), when.day()).unwrap();

                    let transaction_status_meta = result.transaction.meta.unwrap();
                    let fee = transaction_status_meta.fee;

                    let mut account_balance_diff = (|| {
                        if let solana_transaction_status::EncodedTransaction::Json(ui_transaction) =
                            result.transaction.transaction
                        {
                            if let solana_transaction_status::UiMessage::Raw(ui_message) =
                                ui_transaction.message
                            {
                                return izip!(
                                    &ui_message.account_keys,
                                    &transaction_status_meta.pre_balances,
                                    &transaction_status_meta.post_balances
                                )
                                .map(|(address, pre_balance, post_balance)| {
                                    let diff = *post_balance as i64 - *pre_balance as i64;
                                    (address.parse::<Pubkey>().unwrap(), diff)
                                })
                                .collect::<Vec<(Pubkey, i64)>>();
                            }
                        }
                        vec![]
                    })();
                    account_balance_diff[0].1 += fee as i64;
                    let account_balance_diff: BTreeMap<_, _> =
                        account_balance_diff.into_iter().collect();

                    let pre_token_balances =
                        Option::<Vec<_>>::from(transaction_status_meta.pre_token_balances)
                            .unwrap_or_default();
                    let post_token_balances =
                        Option::<Vec<_>>::from(transaction_status_meta.post_token_balances)
                            .unwrap_or_default();

                    let token_amount_diff = |owner: Pubkey, mint: Pubkey| {
                        let owner = owner.to_string();
                        let mint = mint.to_string();

                        let num_token_balances = pre_token_balances
                            .iter()
                            .filter(|token_balance| token_balance.mint == mint)
                            .count();
                        assert_eq!(
                            num_token_balances,
                            post_token_balances
                                .iter()
                                .filter(|token_balance| token_balance.mint == mint)
                                .count()
                        );

                        let pre = pre_token_balances
                            .iter()
                            .filter_map(|token_balance| {
                                if (num_token_balances == 1
                                    || token_balance.owner.as_ref() == Some(&owner).into())
                                    && token_balance.mint == mint
                                {
                                    Some(
                                        token_balance
                                            .ui_token_amount
                                            .amount
                                            .parse::<u64>()
                                            .expect("amount"),
                                    )
                                } else {
                                    None
                                }
                            })
                            .next()
                            .unwrap_or_else(|| {
                                panic!(
                                    "pre_token_balance not found for owner {address}, mint {mint}"
                                )
                            });
                        let post = post_token_balances
                            .iter()
                            .filter_map(|token_balance| {
                                if (num_token_balances == 1
                                    || token_balance.owner.as_ref() == Some(&owner).into())
                                    && token_balance.mint == mint
                                {
                                    Some(
                                        token_balance
                                            .ui_token_amount
                                            .amount
                                            .parse::<u64>()
                                            .expect("amount"),
                                    )
                                } else {
                                    None
                                }
                            })
                            .next()
                            .unwrap_or_else(|| {
                                panic!(
                                    "post_token_balance not found for owner {address},  mint {mint}"
                                )
                            });
                        (post as i64 - pre as i64).unsigned_abs()
                    };

                    let from_amount = if from_token.is_sol() {
                        account_balance_diff
                            .get(&address)
                            .unwrap_or_else(|| {
                                panic!("account_balance_diff not found for owner {address}")
                            })
                            .unsigned_abs()
                    } else {
                        token_amount_diff(address, from_token.mint())
                    };
                    let to_amount = if to_token.is_sol() {
                        account_balance_diff
                            .get(&address)
                            .unwrap_or_else(|| {
                                panic!("account_balance_diff not found for owner {address}")
                            })
                            .unsigned_abs()
                    } else {
                        token_amount_diff(address, to_token.mint())
                    };
                    let msg = format!(
                        "Swapped {}{} into {}{} at {}{} per {}1",
                        from_token.symbol(),
                        from_token
                            .ui_amount(from_amount)
                            .separated_string_with_fixed_place(2),
                        to_token.symbol(),
                        to_token
                            .ui_amount(to_amount)
                            .separated_string_with_fixed_place(2),
                        to_token.symbol(),
                        (to_token.ui_amount(to_amount) / from_token.ui_amount(from_amount))
                            .separated_string_with_fixed_place(2),
                        from_token.symbol(),
                    );
                    db.confirm_swap(signature, when, from_amount, to_amount)?;
                    notifier.send(&msg).await;
                    writeln!(writer, "{msg}")?;
                } else {
                    writeln!(writer, "Pending {swap} failed with {result:?}: {signature}")?;
                    db.cancel_swap(signature)?;
                }
            }
            None => {
                if block_height > last_valid_block_height {
                    writeln!(writer, "Pending {swap} cancelled: {signature}")?;
                    db.cancel_swap(signature)?;
                } else {
                    writeln!(
                        writer,
                        "{} pending for at most {} blocks: {}",
                        swap,
                        last_valid_block_height.saturating_sub(block_height),
                        signature
                    )?;
                }
            }
        }
    }

    Ok(())
}

pub struct LiquidityTokenInfo {
    pub liquidity_token: MaybeToken,
    pub current_liquidity_token_rate: Decimal,
    pub current_apr: Option<f64>,
}

pub fn liquidity_token_ui_amount(
    acquisition_liquidity_ui_amount: Option<f64>,
    ui_amount: f64,
    liquidity_token_info: Option<&LiquidityTokenInfo>,
    include_apr: bool,
) -> (String, String) {
    liquidity_token_info
        .map(
            |LiquidityTokenInfo {
                 liquidity_token,
                 current_liquidity_token_rate,
                 current_apr,
             }| {
                let liquidity_ui_amount = f64::try_from(
                    Decimal::from_f64(ui_amount).unwrap() * current_liquidity_token_rate,
                )
                .unwrap();

                (
                    format!(
                        " [{}{}]",
                        liquidity_token.format_ui_amount(liquidity_ui_amount),
                        match current_apr {
                            Some(current_apr) if include_apr => format!(", {current_apr:.2}% APR"),
                            _ => String::new(),
                        }
                    ),
                    acquisition_liquidity_ui_amount
                        .map(|acquisition_liquidity_ui_amount| {
                            format!(
                                "[{}{}]",
                                liquidity_token.symbol(),
                                (liquidity_ui_amount - acquisition_liquidity_ui_amount)
                                    .separated_string_with_fixed_place(2)
                            )
                        })
                        .unwrap_or_default(),
                )
            },
        )
        .unwrap_or_default()
}

#[allow(clippy::too_many_arguments)]
pub async fn maybe_println_lot<W: Write>(
    token: MaybeToken,
    lot: &Lot,
    current_price: Option<Decimal>,
    liquidity_token_info: Option<&LiquidityTokenInfo>,
    total_basis: &mut f64,
    total_income: &mut f64,
    total_cap_gain: &mut f64,
    long_term_cap_gain: &mut bool,
    total_current_value: &mut f64,
    notifier: Option<&Notifier>,
    verbose: bool,
    print: bool,
    writer: &mut W,
) {
    let current_value = current_price.map(|current_price| {
        f64::try_from(Decimal::from_f64(token.ui_amount(lot.amount)).unwrap() * current_price)
            .unwrap()
    });
    let basis = lot.basis(token);
    let income = lot.income(token);
    let cap_gain = lot.cap_gain(token, current_price.unwrap_or_default());

    let mut acquisition_liquidity_ui_amount = None;
    if let Some(LiquidityTokenInfo {
        liquidity_token, ..
    }) = liquidity_token_info
    {
        if let LotAcquistionKind::Swap { token, amount, .. } = lot.acquisition.kind {
            if !token.fiat_fungible() && token == *liquidity_token {
                if let Some(amount) = amount {
                    acquisition_liquidity_ui_amount = Some(token.ui_amount(amount));
                }
            }
        }
    }

    *total_basis += basis;
    *total_income += income;
    *total_cap_gain += cap_gain;
    *total_current_value += current_value.unwrap_or_default();
    *long_term_cap_gain = is_long_term_cap_gain(lot.acquisition.when, None);

    let ui_amount = token.ui_amount(lot.amount);
    let (liquidity_ui_amount, liquidity_token_cap_gain) = liquidity_token_ui_amount(
        acquisition_liquidity_ui_amount,
        ui_amount,
        liquidity_token_info,
        false,
    );

    let current_value = current_value
        .map(|current_value| {
            format!(
                "value: {:>14}{}",
                current_value.separated_string_with_fixed_place(2),
                liquidity_ui_amount
            )
        })
        .unwrap_or_else(|| "value: ?".into());

    let description = if verbose {
        format!("| {}", lot.acquisition.kind,)
    } else {
        String::new()
    };

    let msg = format!(
        "{:>5}. {} | {:>17} at {:>6} | {} | income: {:>11} | {} gain: {:>14}{} {}",
        lot.lot_number,
        lot.acquisition.when,
        token.format_ui_amount(ui_amount),
        f64::try_from(lot.acquisition.price())
            .unwrap()
            .separated_string_with_fixed_place(2),
        current_value,
        income.separated_string_with_fixed_place(2),
        if *long_term_cap_gain {
            " long"
        } else {
            "short"
        },
        cap_gain.separated_string_with_fixed_place(2),
        liquidity_token_cap_gain,
        description,
    );

    // if !token.fiat_fungible() {

    if let Some(notifier) = notifier {
        notifier.send(&msg).await;
    }

    if print {
        writeln!(writer, "{msg}").unwrap();
    }
    // }
}

#[allow(clippy::too_many_arguments)]
pub async fn process_account_merge<T: Signers, W: Write>(
    db: &mut Db,
    rpc_clients: &RpcClients,
    from_address: Pubkey,
    into_address: Pubkey,
    authority_address: Pubkey,
    signers: T,
    priority_fee: PriorityFee,
    existing_signature: Option<Signature>,
    writer: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_client = rpc_clients.default();
    let token = MaybeToken::SOL(); // TODO: Support merging tokens one day

    if let Some(existing_signature) = existing_signature {
        db.record_transfer(
            existing_signature,
            0, /*last_valid_block_height*/
            None,
            from_address,
            token,
            into_address,
            token,
            LotSelectionMethod::default(),
            None,
        )?;
    } else {
        let (recent_blockhash, last_valid_block_height) =
            rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

        let from_account = rpc_client
            .get_account_with_commitment(&from_address, rpc_client.commitment())?
            .value
            .ok_or_else(|| format!("From account, {from_address}, does not exist"))?;

        let from_tracked_account = db
            .get_account(from_address, token)
            .ok_or_else(|| format!("Account, {from_address}, is not tracked"))?;

        let into_account = rpc_client
            .get_account_with_commitment(&into_address, rpc_client.commitment())?
            .value
            .ok_or_else(|| format!("From account, {into_address}, does not exist"))?;

        let authority_account = if from_address == authority_address {
            from_account.clone()
        } else {
            rpc_client
                .get_account_with_commitment(&authority_address, rpc_client.commitment())?
                .value
                .ok_or_else(|| format!("Authority account, {authority_address}, does not exist"))?
        };

        let amount = from_tracked_account.last_update_balance;

        let mut instructions = if from_account.owner == solana_sdk::stake::program::id()
            && into_account.owner == solana_sdk::stake::program::id()
        {
            solana_sdk::stake::instruction::merge(&into_address, &from_address, &authority_address)
        } else if from_account.owner == solana_sdk::stake::program::id()
            && into_account.owner == system_program::id()
        {
            vec![solana_sdk::stake::instruction::withdraw(
                &from_address,
                &authority_address,
                &into_address,
                amount,
                None,
            )]
        } else {
            return Err(format!(
                "Unsupported merge from {} account to {} account",
                from_account.owner, into_account.owner
            )
            .into());
        };
        apply_priority_fee(rpc_clients, &mut instructions, 10_000, priority_fee)?;

        writeln!(writer, "Merging {from_address} into {into_address}")?;
        if from_address != authority_address {
            writeln!(writer, "Authority address: {authority_address}")?;
        }

        let mut message = Message::new(&instructions, Some(&authority_address));
        message.recent_blockhash = recent_blockhash;
        if rpc_client.get_fee_for_message(&message)? > authority_account.lamports {
            return Err("Insufficient funds for transaction fee".into());
        }

        let mut transaction = Transaction::new_unsigned(message);
        let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
        if simulation_result.err.is_some() {
            return Err(format!("Simulation failure: {simulation_result:?}").into());
        }

        transaction.try_sign(&signers, recent_blockhash)?;
        let signature = transaction.signatures[0];
        writeln!(writer, "Transaction signature: {signature}")?;

        db.record_transfer(
            signature,
            last_valid_block_height,
            Some(amount),
            from_address,
            token,
            into_address,
            token,
            LotSelectionMethod::default(),
            None,
        )?;

        if !send_transaction_until_expired(rpc_clients, &transaction, last_valid_block_height)
            .unwrap_or_default()
        {
            db.cancel_transfer(signature)?;
            return Err("Merge failed".into());
        }
        let when = get_signature_date(rpc_client, signature).await?;
        db.confirm_transfer(signature, when)?;
        db.remove_account(from_address, token)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn process_account_split<T: Signers, W: Write>(
    db: &mut Db,
    rpc_clients: &RpcClients,
    from_address: Pubkey,
    amount: Option<u64>,
    description: Option<String>,
    lot_selection_method: LotSelectionMethod,
    lot_numbers: Option<HashSet<usize>>,
    authority_address: Pubkey,
    signers: T,
    into_keypair: Option<Keypair>,
    if_balance_exceeds: Option<f64>,
    priority_fee: PriorityFee,
    writer: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_client = rpc_clients.default();

    // TODO: Support splitting two system accounts? Tokens? Otherwise at least error cleanly when it's attempted
    let token = MaybeToken::SOL(); // TODO: Support splitting tokens one day

    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

    let into_keypair = into_keypair.unwrap_or_else(Keypair::new);
    if db.get_account(into_keypair.pubkey(), token).is_some() {
        return Err(format!(
            "Account {} ({}) already exists",
            into_keypair.pubkey(),
            token
        )
        .into());
    }

    let from_account = db
        .get_account(from_address, MaybeToken::SOL())
        .ok_or_else(|| format!("SOL account does not exist for {from_address}"))?;

    let (split_all, amount, description) = match amount {
        None => (
            true,
            from_account.last_update_balance,
            description.unwrap_or(from_account.description),
        ),
        Some(amount) => (
            false,
            amount,
            description.unwrap_or_else(|| format!("Split at {}", Local::now())),
        ),
    };

    if let Some(if_balance_exceeds) = if_balance_exceeds {
        if token.ui_amount(amount) < if_balance_exceeds {
            writeln!(
                writer,
                "Split declined because {:?} balance is less than {}",
                from_address,
                token.format_ui_amount(if_balance_exceeds)
            )?;
            return Ok(());
        }
    }

    let minimum_stake_account_balance = rpc_client
        .get_minimum_balance_for_rent_exemption(solana_sdk::stake::state::StakeStateV2::size_of())?;

    let mut instructions = vec![];
    apply_priority_fee(rpc_clients, &mut instructions, 10_000, priority_fee)?;

    instructions.push(system_instruction::transfer(
        &authority_address,
        &into_keypair.pubkey(),
        minimum_stake_account_balance,
    ));
    instructions.append(&mut solana_sdk::stake::instruction::split(
        &from_address,
        &authority_address,
        amount,
        &into_keypair.pubkey(),
    ));

    let message = Message::new(&instructions, Some(&authority_address));

    let mut transaction = Transaction::new_unsigned(message);
    transaction.message.recent_blockhash = recent_blockhash;
    let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
    if simulation_result.err.is_some() {
        return Err(format!("Simulation failure: {simulation_result:?}").into());
    }

    writeln!(
        writer,
        "Splitting {} from {} into {}",
        token.ui_amount(amount),
        from_address,
        into_keypair.pubkey(),
    )?;

    transaction.try_partial_sign(&signers, recent_blockhash)?;
    transaction.try_sign(&[&into_keypair], recent_blockhash)?;

    let signature = transaction.signatures[0];
    writeln!(writer, "Transaction signature: {signature}")?;

    let epoch = rpc_client.get_epoch_info()?.epoch;
    db.add_account(TrackedAccount {
        address: into_keypair.pubkey(),
        token,
        description,
        last_update_epoch: epoch.saturating_sub(1),
        last_update_balance: 0,
        lots: vec![],
        no_sync: from_account.no_sync,
    })?;
    db.record_transfer(
        signature,
        last_valid_block_height,
        Some(amount),
        from_address,
        token,
        into_keypair.pubkey(),
        token,
        lot_selection_method,
        lot_numbers,
    )?;

    if !send_transaction_until_expired(rpc_clients, &transaction, last_valid_block_height)
        .unwrap_or_default()
    {
        db.cancel_transfer(signature)?;
        db.remove_account(into_keypair.pubkey(), MaybeToken::SOL())?;
        return Err("Split failed".into());
    }
    writeln!(writer, "Split confirmed: {signature}")?;
    let when = get_signature_date(rpc_client, signature).await?;
    db.confirm_transfer(signature, when)?;
    if split_all {
        // TODO: This `remove_account` is racy and won't work in all cases. Consider plumbing the
        // removal through `confirm_transfer` instead
        let from_account = db.get_account(from_address, MaybeToken::SOL()).unwrap();
        assert!(from_account.lots.is_empty());
        db.remove_account(from_address, MaybeToken::SOL())?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn process_account_sync<W: Write>(
    db: &mut Db,
    rpc_clients: &RpcClients,
    address: Option<Pubkey>,
    max_epochs_to_process: Option<u64>,
    reconcile_no_sync_account_balances: bool,
    force_rescan_balances: bool,
    notifier: &Notifier,
    writer: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_client = rpc_clients.default();
    process_account_sync_pending_transfers(db, rpc_client, writer).await?;
    process_account_sync_sweep(db, rpc_clients, notifier, writer).await?;

    let (mut accounts, mut no_sync_accounts): (_, Vec<_>) = match address {
        Some(address) => {
            // sync all tokens for the given address...
            let accounts = db.get_account_tokens(address);
            if accounts.is_empty() {
                return Err(format!("{address} does not exist").into());
            }
            accounts
        }
        None => db.get_accounts(),
    }
    .into_iter()
    .partition(|account| !account.no_sync.unwrap_or_default());

    if reconcile_no_sync_account_balances {
        for account in no_sync_accounts.iter_mut() {
            if account.lots.is_empty() {
                continue;
            }

            let current_balance = account.token.balance(rpc_client, &account.address)?;

            match current_balance.cmp(&account.last_update_balance) {
                std::cmp::Ordering::Less => {
                    writeln!(
                        writer,
                        "\nWarning: {} ({}) balance is less than expected. Actual: {}{}, expected: {}{}\n",
                        account.address,
                        account.token,
                        account.token.symbol(),
                        account.token.ui_amount(current_balance),
                        account.token.symbol(),
                        account.token.ui_amount(account.last_update_balance)
                    )?;
                }
                std::cmp::Ordering::Greater => {
                    // sort by lowest basis
                    account
                        .lots
                        .sort_by(|a, b| a.acquisition.price().cmp(&b.acquisition.price()));

                    let lowest_basis_lot = &mut account.lots[0];
                    let additional_balance = current_balance - account.last_update_balance;
                    lowest_basis_lot.amount += additional_balance;

                    let msg = format!(
                        "{} ({}): Additional {}{} added",
                        account.address,
                        account.token,
                        account.token.symbol(),
                        account.token.ui_amount(additional_balance)
                    );
                    notifier.send(&msg).await;
                    writeln!(writer, "{msg}")?;

                    account.last_update_balance = current_balance;
                    db.update_account(account.clone())?;
                }
                _ => {}
            }
        }
    }

    let current_sol_price = MaybeToken::SOL().get_current_price(rpc_client).await?;

    let addresses: Vec<Pubkey> = accounts
        .iter()
        .map(|TrackedAccount { address, .. }| *address)
        .collect::<Vec<_>>();

    let epoch_info = rpc_client.get_epoch_info()?;
    let mut stop_epoch = epoch_info.epoch.saturating_sub(1);

    let start_epoch = accounts
        .iter()
        .map(
            |TrackedAccount {
                 last_update_epoch, ..
             }| last_update_epoch,
        )
        .min()
        .unwrap_or(&stop_epoch)
        + 1;

    if start_epoch > stop_epoch && !force_rescan_balances {
        writeln!(writer, "Processed up to epoch {stop_epoch}")?;
        return Ok(());
    }

    if let Some(max_epochs_to_process) = max_epochs_to_process {
        if max_epochs_to_process == 0 && !force_rescan_balances {
            return Ok(());
        }
        stop_epoch = stop_epoch.min(start_epoch.saturating_add(max_epochs_to_process - 1));
    }

    // Look for inflationary rewards
    for epoch in start_epoch..=stop_epoch {
        let msg = format!("Processing epoch: {epoch}");
        notifier.send(&msg).await;
        writeln!(writer, "{msg}")?;

        let inflation_rewards = rpc_client.get_inflation_reward(&addresses, Some(epoch))?;

        for (inflation_reward, address, account) in
            itertools::izip!(inflation_rewards, addresses.iter(), accounts.iter_mut(),)
        {
            assert_eq!(*address, account.address);
            if account.last_update_epoch >= epoch {
                continue;
            }

            if let Some(inflation_reward) = inflation_reward {
                assert!(!account.token.is_token()); // Only SOL accounts can receive inflationary rewards

                account.last_update_balance += inflation_reward.amount;

                let slot = inflation_reward.effective_slot;
                let (when, price) =
                    get_block_date_and_price(rpc_client, slot, account.token).await?;
                let lot = Lot {
                    lot_number: db.next_lot_number(),
                    acquisition: LotAcquistion::new(
                        when,
                        price,
                        LotAcquistionKind::EpochReward { epoch, slot },
                    ),
                    amount: inflation_reward.amount,
                };

                let msg = format!("{}: {}", account.address, account.description);
                notifier.send(&msg).await;
                writeln!(writer, "{msg}")?;

                maybe_println_lot(
                    account.token,
                    &lot,
                    Some(current_sol_price),
                    None,
                    &mut 0.,
                    &mut 0.,
                    &mut 0.,
                    &mut false,
                    &mut 0.,
                    Some(notifier),
                    true,
                    true,
                    writer,
                )
                .await;
                account.lots.push(lot);
            }
        }
    }

    // Look for unexpected balance changes (such as transaction and rent rewards)
    for account in accounts.iter_mut() {
        account.last_update_epoch = stop_epoch;

        let current_balance = account.token.balance(rpc_client, &account.address)?;
        if current_balance < account.last_update_balance {
            writeln!(
                writer,
                "\nWarning: {} ({}) balance is less than expected. Actual: {}{}, expected: {}{}\n",
                account.address,
                account.token,
                account.token.symbol(),
                account.token.ui_amount(current_balance),
                account.token.symbol(),
                account.token.ui_amount(account.last_update_balance)
            )?;
        } else if current_balance > account.last_update_balance + account.token.amount(0.005) {
            let slot = epoch_info.absolute_slot;
            let current_token_price = account.token.get_current_price(rpc_client).await?;
            let (when, decimal_price) =
                get_block_date_and_price(rpc_client, slot, account.token).await?;
            let amount = current_balance - account.last_update_balance;

            let lot = Lot {
                lot_number: db.next_lot_number(),
                acquisition: LotAcquistion::new(
                    when,
                    decimal_price,
                    LotAcquistionKind::NotAvailable,
                ),
                amount,
            };

            let msg = format!(
                "{} ({}): {}",
                account.address, account.token, account.description
            );
            notifier.send(&msg).await;
            writeln!(writer, "{msg}")?;

            maybe_println_lot(
                account.token,
                &lot,
                Some(current_token_price),
                None,
                &mut 0.,
                &mut 0.,
                &mut 0.,
                &mut false,
                &mut 0.,
                Some(notifier),
                true,
                true,
                writer,
            )
            .await;
            account.lots.push(lot);
            account.last_update_balance = current_balance;
        }

        db.update_account(account.clone())?;
    }

    Ok(())
}

async fn process_account_sync_pending_transfers<W: Write>(
    db: &mut Db,
    rpc_client: &RpcClient,
    writer: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    let block_height = rpc_client.get_epoch_info()?.block_height;
    for PendingTransfer {
        signature,
        last_valid_block_height,
        ..
    } in db.pending_transfers()
    {
        let status = rpc_client.get_signature_status_with_commitment_and_history(
            &signature,
            rpc_client.commitment(),
            true,
        )?;
        match status {
            Some(result) => {
                if result.is_ok() {
                    writeln!(writer, "Pending transfer confirmed: {signature}")?;
                    let when = get_signature_date(rpc_client, signature).await?;
                    db.confirm_transfer(signature, when)?;
                } else {
                    writeln!(
                        writer,
                        "Pending transfer failed with {result:?}: {signature}"
                    )?;
                    db.cancel_transfer(signature)?;
                }
            }
            None => {
                if block_height > last_valid_block_height {
                    writeln!(writer, "Pending transfer cancelled: {signature}")?;
                    db.cancel_transfer(signature)?;
                } else {
                    writeln!(
                        writer,
                        "Transfer pending for at most {} blocks: {}",
                        last_valid_block_height.saturating_sub(block_height),
                        signature
                    )?;
                }
            }
        }
    }
    Ok(())
}

async fn process_account_sync_sweep<W: Write>(
    db: &mut Db,
    rpc_clients: &RpcClients,
    _notifier: &Notifier,
    writer: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_client = rpc_clients.default();
    let token = MaybeToken::SOL();

    let transitory_sweep_stake_addresses = db.get_transitory_sweep_stake_addresses();
    if transitory_sweep_stake_addresses.is_empty() {
        return Ok(());
    }

    let sweep_stake_account_info = db
        .get_sweep_stake_account()
        .ok_or("Sweep stake account is not configured")?;

    let sweep_stake_account_authority_keypair =
        read_keypair_file(&sweep_stake_account_info.stake_authority).map_err(|err| {
            format!(
                "Failed to read {}: {}",
                sweep_stake_account_info.stake_authority.display(),
                err
            )
        })?;

    let sweep_stake_account = rpc_client
        .get_account_with_commitment(&sweep_stake_account_info.address, rpc_client.commitment())?
        .value
        .ok_or("Sweep stake account does not exist")?;

    let sweep_stake_activation_state = get_stake_activation_state(rpc_client, &sweep_stake_account)
        .map_err(|err| {
            format!(
                "Unable to get activation information for sweep stake account: {}: {}",
                sweep_stake_account_info.address, err
            )
        })?;

    if sweep_stake_activation_state != StakeActivationState::Active {
        writeln!(
            writer,
            "Sweep stake account is not active, unable to continue: {sweep_stake_activation_state:?}"
        )?;
        return Ok(());
    }

    for transitory_sweep_stake_address in transitory_sweep_stake_addresses {
        writeln!(
            writer,
            "Considering merging transitory stake {transitory_sweep_stake_address}"
        )?;

        let transitory_sweep_stake_account = match rpc_client
            .get_account_with_commitment(&transitory_sweep_stake_address, rpc_client.commitment())?
            .value
        {
            None => {
                writeln!(
                    writer,
                    "  Transitory sweep stake account does not exist, removing it: {transitory_sweep_stake_address}"
                )?;

                if let Some(tracked_account) = db.get_account(transitory_sweep_stake_address, token)
                {
                    if tracked_account.last_update_balance > 0 || !tracked_account.lots.is_empty() {
                        panic!("Tracked account is not empty: {tracked_account:?}");

                        // TODO: Simulate a transfer to move the lots into the sweep account in
                        // this case?
                        /*
                        let signature = Signature::default();
                        db.record_transfer(
                            signature,
                            None,
                            transitory_sweep_stake_address,
                            sweep_stake_account_info.address,
                            None,
                        )?;
                        db.confirm_transfer(signature)?;
                        */
                    }
                }
                db.remove_transitory_sweep_stake_address(transitory_sweep_stake_address)?;
                continue;
            }
            Some(x) => x,
        };

        let transient_stake_activation_state = get_stake_activation_state(
            rpc_client,
            &transitory_sweep_stake_account,
        )
            .map_err(|err| {
                format!(
                    "Unable to get activation information for transient stake: {transitory_sweep_stake_address}: {err}"
                )
            })?;

        if transient_stake_activation_state != StakeActivationState::Active {
            println!("  Transitory stake is not yet active: {transient_stake_activation_state:?}");
            continue;
        }

        if !rpc_client_utils::stake_accounts_have_same_credits_observed(
            &sweep_stake_account,
            &transitory_sweep_stake_account,
        )? {
            writeln!(
                writer,
                "  Transitory stake credits observed mismatch with sweep stake account: {transitory_sweep_stake_address}"
            )?;
            continue;
        }
        writeln!(writer, "  Merging into sweep stake account")?;

        let message = Message::new(
            &solana_sdk::stake::instruction::merge(
                &sweep_stake_account_info.address,
                &transitory_sweep_stake_address,
                &sweep_stake_account_authority_keypair.pubkey(),
            ),
            Some(&sweep_stake_account_authority_keypair.pubkey()),
        );
        let mut transaction = Transaction::new_unsigned(message);

        let (recent_blockhash, last_valid_block_height) =
            rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

        transaction.message.recent_blockhash = recent_blockhash;
        let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
        if simulation_result.err.is_some() {
            return Err(format!("Simulation failure: {simulation_result:?}").into());
        }

        transaction.sign(&[&sweep_stake_account_authority_keypair], recent_blockhash);

        let signature = transaction.signatures[0];
        writeln!(writer, "Transaction signature: {signature}")?;
        db.record_transfer(
            signature,
            last_valid_block_height,
            None,
            transitory_sweep_stake_address,
            token,
            sweep_stake_account_info.address,
            token,
            LotSelectionMethod::default(),
            None,
        )?;

        if !send_transaction_until_expired(rpc_clients, &transaction, last_valid_block_height)
            .unwrap_or_default()
        {
            db.cancel_transfer(signature)?;
            return Err("Merge failed".into());
        }
        let when = get_signature_date(rpc_client, signature).await?;
        db.confirm_transfer(signature, when)?;
        db.remove_transitory_sweep_stake_address(transitory_sweep_stake_address)?;
    }
    Ok(())
}
