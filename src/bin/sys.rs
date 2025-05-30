use {
    chrono::prelude::*,
    chrono_humanize::HumanTime,
    clap::{
        crate_description, crate_name, value_t, value_t_or_exit, values_t, App, AppSettings, Arg,
        ArgMatches, SubCommand,
    },
    console::{style, Style},
    db::*,
    itertools::{izip, Itertools},
    rpc_client_utils::get_signature_date,
    rust_decimal::prelude::*,
    separator::FixedPlaceSeparatable,
    solana_clap_utils::{self, input_parsers::*, input_validators::*},
    solana_client::{
        rpc_client::RpcClient, rpc_config::RpcTransactionConfig, rpc_response::StakeActivationState,
    },
    solana_sdk::{
        clock::Slot,
        compute_budget,
        message::Message,
        native_token::{sol_to_lamports, Sol},
        pubkey::Pubkey,
        signature::{read_keypair_file, Keypair, Signature, Signer},
        signers::Signers,
        stake::state::Authorized,
        system_instruction, system_program,
        transaction::Transaction,
    },
    std::{
        collections::{BTreeMap, HashSet},
        fs,
        path::PathBuf,
        process::exit,
        str::FromStr,
        thread::sleep,
        time::Duration,
    },
    sys::{
        amount::Amount,
        exchange::{self, *},
        get_transaction_balance_change::*,
        metrics::{self, dp, MetricsConfig},
        notifier::*,
        priority_fee::{apply_priority_fee, PriorityFee},
        send_transaction_until_expired,
        token::*,
        *,
    },
};

fn is_amount_or_all_or_half<T>(amount: T) -> Result<(), String>
where
    T: AsRef<str> + std::fmt::Display,
{
    if amount.as_ref().parse::<u64>().is_ok()
        || amount.as_ref().parse::<f64>().is_ok()
        || amount.as_ref() == "ALL"
        || amount.as_ref() == "HALF"
    {
        Ok(())
    } else {
        Err(format!(
            "Unable to parse input amount as integer or float, provided: {amount}"
        ))
    }
}

pub(crate) fn today() -> NaiveDate {
    let today = Local::now().date_naive();
    NaiveDate::from_ymd_opt(today.year(), today.month(), today.day()).unwrap()
}

fn is_long_term_cap_gain(acquisition: NaiveDate, disposal: Option<NaiveDate>) -> bool {
    let disposal = disposal.unwrap_or_else(today);
    let hold_time = disposal - acquisition;
    hold_time >= chrono::Duration::try_days(365).unwrap()
}

fn format_order_side(order_side: OrderSide) -> String {
    match order_side {
        OrderSide::Buy => style(" Buy").green(),
        OrderSide::Sell => style("Sell").red(),
    }
    .to_string()
}

fn format_filled_amount(filled_amount: f64) -> String {
    if filled_amount == 0. {
        Style::new()
    } else {
        Style::new().bold()
    }
    .apply_to(format!(" [◎{filled_amount} filled]"))
    .to_string()
}

fn naivedate_of(string: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(string, "%y/%m/%d")
        .or_else(|_| NaiveDate::parse_from_str(string, "%Y/%m/%d"))
        .map_err(|err| format!("error parsing '{string}': {err}"))
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

fn add_exchange_deposit_address_to_db(
    db: &mut Db,
    exchange: Exchange,
    exchange_account: &str,
    token: MaybeToken,
    deposit_address: Pubkey,
    rpc_client: &RpcClient,
) -> Result<(), Box<dyn std::error::Error>> {
    if db.get_account(deposit_address, token).is_none() {
        let epoch = rpc_client.get_epoch_info()?.epoch;
        db.add_account(TrackedAccount {
            address: deposit_address,
            token,
            description: format!("{exchange:?} {exchange_account}"),
            last_update_epoch: epoch,
            last_update_balance: 0,
            lots: vec![],
            no_sync: Some(true),
        })?;
    }
    Ok(())
}

async fn process_sync_exchange(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    rpc_client: &RpcClient,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let recent_deposits = exchange_client.recent_deposits().await?;
    let recent_withdrawals = exchange_client.recent_withdrawals().await?;

    let epoch_info = rpc_client.get_epoch_info_with_commitment(rpc_client.commitment())?;

    for pending_withdrawal in db.pending_withdrawals(Some(exchange)) {
        let wi = recent_withdrawals
            .iter()
            .find(|wi| wi.tag == pending_withdrawal.tag)
            .unwrap_or_else(|| {
                panic!("Unknown pending withdrawal: {}", pending_withdrawal.tag);
            });

        let token = pending_withdrawal.token;

        if wi.completed {
            if let Some(ref tx_id) = wi.tx_id {
                metrics::push(dp::exchange_withdrawal(
                    exchange,
                    token,
                    &wi.address,
                    token.ui_amount(pending_withdrawal.amount),
                ))
                .await;
                let msg = format!(
                    "{} {}{} withdrawal to {} successful ({})",
                    token,
                    token.symbol(),
                    token
                        .ui_amount(pending_withdrawal.amount)
                        .separated_string_with_fixed_place(2),
                    wi.address,
                    tx_id,
                );
                println!("{msg}");

                db.confirm_withdrawal(pending_withdrawal, today())?;
                notifier.send(&format!("{exchange:?}: {msg}")).await;
            } else {
                println!("Pending {} withdrawal to {} cancelled", token, wi.address);
                db.cancel_withdrawal(pending_withdrawal)?;
            }
        } else {
            println!(
                "{} {}{} withdrawal to {} pending",
                token,
                token.symbol(),
                token.ui_amount(pending_withdrawal.amount),
                wi.address,
            );
        }
    }

    for pending_deposit in db.pending_deposits(Some(exchange)) {
        let response = rpc_client
            .get_signature_statuses_with_history(&[pending_deposit.transfer.signature])?;
        if response.context.slot < epoch_info.absolute_slot {
            // TODO: Recover gracefully, probably by just skipping this pending deposit
            panic!(
                "RPC node is acting weird. Broken load balancer? ({} < {})",
                response.context.slot, epoch_info.absolute_slot
            );
        }
        let (confirmed, ok) = response.value[0]
            .as_ref()
            .map(|status| {
                (
                    status.satisfies_commitment(rpc_client.commitment()),
                    status.err.is_none(),
                )
            })
            .unwrap_or_default();

        assert_eq!(
            pending_deposit.transfer.from_token,
            pending_deposit.transfer.to_token
        );
        let token = pending_deposit.transfer.to_token;

        if confirmed && ok {
            metrics::push(dp::exchange_deposit(
                exchange,
                token,
                token.ui_amount(pending_deposit.amount),
            ))
            .await;
            println!(
                "{} {}{} deposit pending ({} confirmed)",
                token,
                token.symbol(),
                token.ui_amount(pending_deposit.amount),
                pending_deposit.transfer.signature,
            );
            match recent_deposits.as_ref() {
                None => {
                    if token.fiat_fungible() {
                        db.drop_deposit(pending_deposit.transfer.signature)?;

                        let msg = format!(
                            "{} {}{} BLIND deposit successful ({})",
                            token,
                            token.symbol(),
                            token.ui_amount(pending_deposit.amount),
                            pending_deposit.transfer.signature
                        );
                        println!("{msg}");
                        notifier.send(&format!("{exchange:?}: {msg}")).await;
                    } else {
                        // Refuse to forget these lots, there may be a tax implication with doing
                        // so.
                        panic!("Fix exchange implementation");
                    }
                }
                Some(recent_deposits) => {
                    if let Some(deposit_info) = recent_deposits.iter().find(|deposit_info| {
                        deposit_info.tx_id == pending_deposit.transfer.signature.to_string()
                    }) {
                        let missing_tokens = (token.amount(deposit_info.amount) as i64
                            - (pending_deposit.amount as i64))
                            .abs();
                        if missing_tokens >= 10 {
                            let msg = format!(
                                "Error! {} deposit amount mismatch for {}! Actual amount: ◎{}, expected amount: ◎{}",
                                token,
                                pending_deposit.transfer.signature, deposit_info.amount, pending_deposit.amount
                            );
                            println!("{msg}");
                            notifier.send(&format!("{exchange:?}: {msg}")).await;

                            // TODO: Do something more here...?
                        } else {
                            if missing_tokens != 0 {
                                // Binance will occasionally steal a lamport or two...
                                let msg = format!(
                                    "{exchange:?} just stole {missing_tokens} tokens from your deposit!"
                                );
                                println!("{msg}");
                                notifier.send(&format!("{exchange:?}: {msg}")).await;
                            }

                            let when =
                                get_signature_date(rpc_client, pending_deposit.transfer.signature)
                                    .await?;
                            db.confirm_deposit(pending_deposit.transfer.signature, when)?;

                            let msg = format!(
                                "{} {}{} deposit successful ({})",
                                token,
                                token.symbol(),
                                token.ui_amount(pending_deposit.amount),
                                pending_deposit.transfer.signature
                            );
                            println!("{msg}");
                            notifier.send(&format!("{exchange:?}: {msg}")).await;
                        }
                    }
                }
            }
        } else if !ok {
            println!(
                "Pending {} deposit failed: {}",
                token, pending_deposit.transfer.signature
            );
            db.cancel_deposit(pending_deposit.transfer.signature)
                .expect("cancel_deposit");
        } else if epoch_info.block_height > pending_deposit.transfer.last_valid_block_height {
            println!(
                "Pending {} deposit expired: {}",
                token, pending_deposit.transfer.signature
            );
            db.cancel_deposit(pending_deposit.transfer.signature)
                .expect("cancel_deposit");
        } else {
            println!(
                "{} {}{} deposit pending for at most {} blocks ({} unconfirmed)",
                token,
                token.symbol(),
                token.ui_amount(pending_deposit.amount),
                pending_deposit
                    .transfer
                    .last_valid_block_height
                    .saturating_sub(epoch_info.block_height),
                pending_deposit.transfer.signature,
            );
        }
    }

    for order_info in db.open_orders(Some(exchange), None) {
        let token = order_info.token;
        let order_status = exchange_client
            .order_status(&order_info.pair, &order_info.order_id)
            .await?;
        let order_summary = format!(
            "{}: {} {} {}{:<5} at ${:<.2}{} | id {} created {}",
            order_info.pair,
            token,
            format_order_side(order_info.side),
            token.symbol(),
            order_status.amount,
            order_status.price,
            if order_status.filled_amount == 0. {
                String::default()
            } else {
                format_filled_amount(order_status.filled_amount)
            },
            order_info.order_id,
            HumanTime::from(order_info.creation_time),
        );

        if order_status.open {
            if order_status.filled_amount > 0. {
                let msg = format!("Partial {order_summary}");
                println!("{msg}");
                notifier.send(&format!("{exchange:?}: {msg}")).await;
            } else {
                println!("   Open {order_summary}");
            }
        } else {
            let fee_summary = match &order_status.fee {
                Some((amount, coin)) if *amount > 0. => format!(" (fee: {amount} {coin})"),
                _ => "".into(),
            };
            db.close_order(
                &order_info.order_id,
                token.amount(order_status.amount),
                token.amount(order_status.filled_amount),
                order_status.price,
                order_status.last_update,
                order_status.fee,
            )?;

            if order_status.filled_amount > f64::EPSILON {
                metrics::push(dp::exchange_fill(
                    exchange,
                    &order_info.pair,
                    order_info.side,
                    token,
                    order_status.filled_amount,
                    order_status.price,
                ))
                .await;
            }

            let msg = if (order_status.amount - order_status.filled_amount).abs() < f64::EPSILON {
                format!(" Filled {order_summary}{fee_summary}")
            } else if order_status.filled_amount < f64::EPSILON {
                format!(" Cancel {order_summary}{fee_summary}")
            } else {
                format!("Partial {order_summary}{fee_summary}")
            };
            println!("{msg}");
            notifier.send(&format!("{exchange:?}: {msg}")).await;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_exchange_deposit<T: Signers>(
    db: &mut Db,
    rpc_clients: &RpcClients,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    token: MaybeToken,
    deposit_address: Pubkey,
    amount: Amount,
    from_address: Pubkey,
    if_source_balance_exceeds: Option<u64>,
    if_exchange_balance_less_than: Option<u64>,
    authority_address: Pubkey,
    signers: T,
    lot_selection_method: LotSelectionMethod,
    lot_numbers: Option<HashSet<usize>>,
    priority_fee: PriorityFee,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_client = rpc_clients.default();

    if let Some(if_exchange_balance_less_than) = if_exchange_balance_less_than {
        let exchange_balance = exchange_client
            .balances()
            .await?
            .get(&token.to_string())
            .map(|b| token.amount(b.total))
            .unwrap_or(0)
            + db.pending_deposits(Some(exchange))
                .into_iter()
                .map(|pd| pd.amount)
                .sum::<u64>();

        if exchange_balance < if_exchange_balance_less_than {
            println!(
                "{0} deposit declined because {1:?} balance ({4}{2}) is less than {4}{3}",
                token,
                exchange,
                token.ui_amount(exchange_balance),
                token.ui_amount(if_exchange_balance_less_than),
                token.symbol(),
            );
            return Ok(());
        }
    }

    let from_tracked_account = db
        .get_account(from_address, token)
        .ok_or_else(|| format!("Account, {from_address}, is not tracked"))?;
    let from_account_balance = from_tracked_account.last_update_balance;

    if let Some(if_source_balance_exceeds) = if_source_balance_exceeds {
        if from_account_balance < if_source_balance_exceeds {
            println!(
                "{} deposit declined because {} balance is less than {}{}",
                token,
                from_address,
                token.symbol(),
                token.ui_amount(if_source_balance_exceeds)
            );
            return Ok(());
        }
    }

    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

    let from_account = rpc_client
        .get_account_with_commitment(&from_address, rpc_client.commitment())?
        .value
        .ok_or_else(|| format!("From account, {from_address}, does not exist"))?;

    let authority_account = if from_address == authority_address {
        #[allow(clippy::redundant_clone)]
        from_account.clone()
    } else {
        rpc_client
            .get_account_with_commitment(&authority_address, rpc_client.commitment())?
            .value
            .ok_or_else(|| format!("Authority account, {authority_address}, does not exist"))?
    };

    let (mut instructions, amount, compute_units) = match token.token() {
        /*SOL*/
        None => {
            assert_eq!(from_account.lamports, from_account_balance);

            if from_account.owner == system_program::id() {
                let fee = if from_address == authority_address {
                    let dummy_message = Message::new_with_blockhash(
                        &[system_instruction::transfer(
                            &from_address,
                            &deposit_address,
                            0,
                        )],
                        Some(&authority_address),
                        &recent_blockhash,
                    );
                    rpc_client.get_fee_for_message(&dummy_message)?
                } else {
                    0
                };
                let amount = amount.unwrap_or_else(|| from_account_balance.saturating_sub(fee));

                (
                    vec![system_instruction::transfer(
                        &from_address,
                        &deposit_address,
                        amount,
                    )],
                    amount,
                    1_000,
                )
            } else if from_account.owner == solana_program::vote::program::id() {
                let minimum_balance = rpc_client.get_minimum_balance_for_rent_exemption(
                    solana_program::vote::state::VoteState::size_of(),
                )?;

                let amount =
                    amount.unwrap_or_else(|| from_account_balance.saturating_sub(minimum_balance));

                (
                    vec![solana_program::vote::instruction::withdraw(
                        &from_address,
                        &authority_address,
                        amount,
                        &deposit_address,
                    )],
                    amount,
                    1_000,
                )
            } else if from_account.owner == solana_sdk::stake::program::id() {
                let amount = amount.unwrap_or(from_account_balance);

                (
                    vec![solana_sdk::stake::instruction::withdraw(
                        &from_address,
                        &authority_address,
                        &deposit_address,
                        amount,
                        None,
                    )],
                    amount,
                    1_000,
                )
            } else {
                return Err(
                    format!("Unsupported `from` account owner: {}", from_account.owner).into(),
                );
            }
        }
        Some(token) => {
            let amount = amount.unwrap_or(from_account_balance);

            let mut instructions = vec![];

            let mut compute_units = 7_500;

            if rpc_client
                .get_account_with_commitment(&token.ata(&deposit_address), rpc_client.commitment())?
                .value
                .is_none()
            {
                instructions.push(
                    spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                        &authority_address,
                        &deposit_address,
                        &token.mint(),
                        &token.program_id(),
                    ),
                );
                compute_units += 20_000
            }

            instructions.push(
                spl_token::instruction::transfer_checked(
                    &token.program_id(),
                    &token.ata(&from_address),
                    &token.mint(),
                    &token.ata(&deposit_address),
                    &authority_address,
                    &[],
                    amount,
                    token.decimals(),
                )
                .unwrap(),
            );

            (instructions, amount, compute_units)
        }
    };
    apply_priority_fee(rpc_clients, &mut instructions, compute_units, priority_fee)?;

    if amount == 0 {
        return Err("Nothing to deposit".into());
    }
    if from_account_balance < amount {
        return Err("From account has insufficient funds".into());
    }

    println!("From address: {from_address} ({token})");
    if from_address != authority_address {
        println!("Authority address: {authority_address}");
    }
    println!("Amount: {}{}", token.symbol(), token.ui_amount(amount));
    println!("{token} {exchange:?} deposit address: {deposit_address}");

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
    println!("Transaction signature: {signature}");

    db.record_deposit(
        signature,
        last_valid_block_height,
        from_address,
        amount,
        exchange,
        deposit_address,
        token,
        lot_selection_method,
        lot_numbers,
    )?;
    if !send_transaction_until_expired(rpc_clients, &transaction, last_valid_block_height)
        .unwrap_or_default()
    {
        return Err("Deposit failed".into());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_exchange_withdraw(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    token: MaybeToken,
    deposit_address: Pubkey,
    amount: Option<u64>,
    to_address: Pubkey,
    lot_selection_method: LotSelectionMethod,
    lot_numbers: Option<HashSet<usize>>,
    withdrawal_password: Option<String>,
    withdrawal_code: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let deposit_account = db
        .get_account(deposit_address, token)
        .expect("unknown deposit address");
    let _to_account = db
        .get_account(to_address, token)
        .expect("unknown to address");

    let amount = amount.unwrap_or(deposit_account.last_update_balance);

    let (tag, fee_as_ui_amount) = exchange_client
        .request_withdraw(
            to_address,
            token,
            token.ui_amount(amount),
            withdrawal_password,
            withdrawal_code,
        )
        .await?;

    let fee = token.amount(fee_as_ui_amount);
    db.record_withdrawal(
        exchange,
        tag,
        token,
        amount,
        fee,
        deposit_address,
        to_address,
        lot_selection_method,
        lot_numbers,
    )?;
    Ok(())
}

enum LimitOrderPrice {
    At(f64),
    AmountOverAsk(f64),
    AmountUnderBid(f64),
}

#[allow(clippy::too_many_arguments)]
async fn process_exchange_cancel(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    order_ids: HashSet<String>,
    max_create_time: Option<DateTime<Utc>>,
    side: Option<OrderSide>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cancelled_count = 0;
    for order_info in db.open_orders(Some(exchange), side) {
        let mut cancel = false;
        if order_ids.contains(&order_info.order_id) {
            cancel = true;
        }

        if let Some(ref max_create_time) = max_create_time {
            if order_info.creation_time < *max_create_time {
                cancel = true;
            }
        }

        if cancel {
            println!("Cancelling order {}", order_info.order_id);
            cancelled_count += 1;
            exchange_client
                .cancel_order(&order_info.pair, &order_info.order_id)
                .await
                .unwrap_or_else(|err| eprintln!("{err:?}"));
        }
    }

    println!("{cancelled_count} orders cancelled");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_exchange_buy(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    token: MaybeToken,
    pair: String,
    amount: Option<f64>,
    price: LimitOrderPrice,
    if_balance_exceeds: Option<f64>,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let bid_ask = exchange_client.bid_ask(&pair).await?;
    println!(
        "{} | Ask: ${}, Bid: ${}",
        pair, bid_ask.ask_price, bid_ask.bid_price
    );

    let deposit_address = exchange_client.deposit_address(token).await?;
    let deposit_account = db.get_account(deposit_address, token).ok_or_else(|| {
        format!(
            "Exchange deposit account does not exist, run `sync` first: {deposit_address} ({token})",
        )
    })?;

    let balances = exchange_client.balances().await?;
    let usd_balance = balances.get("USD").cloned().unwrap_or_default().available;

    if let Some(if_balance_exceeds) = if_balance_exceeds {
        if usd_balance < if_balance_exceeds {
            println!(
                "Order declined because {exchange:?} available balance is less than ${if_balance_exceeds}"
            );
            return Ok(());
        }
    }

    let price = match price {
        LimitOrderPrice::At(price) => price,
        LimitOrderPrice::AmountOverAsk(_) => panic!("Bug: AmountOverAsk invalid for a buy order"),
        LimitOrderPrice::AmountUnderBid(extra) => bid_ask.bid_price - extra,
    };
    let price = (price * 10_000.).round() / 10_000.; // Round to four decimal places

    if price > bid_ask.bid_price {
        return Err(format!("Order price, {price}, is greater than bid price").into());
    }

    let amount = match amount {
        None => (usd_balance / price).floor(),
        Some(amount) => amount,
    };

    println!("Placing buy order for ◎{amount} at ${price}");

    let order_id = exchange_client
        .place_order(&pair, OrderSide::Buy, price, amount)
        .await?;
    let msg = format!(
        "Order created: {}: {:?} ◎{} at ${}, id {}",
        pair,
        OrderSide::Buy,
        amount,
        price,
        order_id,
    );
    db.open_order(
        OrderSide::Buy,
        deposit_account,
        exchange,
        pair,
        price,
        order_id,
        vec![],
        Some(amount),
    )?;
    println!("{msg}");
    notifier.send(&format!("{exchange:?}: {msg}")).await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_exchange_sell(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    token: MaybeToken,
    pair: String,
    amount: f64,
    price: LimitOrderPrice,
    if_balance_exceeds: Option<u64>,
    if_price_over: Option<f64>,
    if_price_over_basis: bool,
    price_floor: Option<f64>,
    lot_selection_method: LotSelectionMethod,
    lot_numbers: Option<HashSet<usize>>,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let bid_ask = exchange_client.bid_ask(&pair).await?;
    println!(
        "{} | Ask: ${}, Bid: ${}",
        pair, bid_ask.ask_price, bid_ask.bid_price
    );

    let deposit_address = exchange_client.deposit_address(token).await?;
    let mut deposit_account = db.get_account(deposit_address, token).ok_or_else(|| {
        format!(
            "Exchange deposit account does not exist, run `sync` first: {deposit_address} ({token})",
        )
    })?;

    if let Some(if_balance_exceeds) = if_balance_exceeds {
        if deposit_account.last_update_balance < if_balance_exceeds {
            println!(
                "Order declined because {:?} available balance is less than {}",
                exchange,
                token.ui_amount(if_balance_exceeds)
            );
            return Ok(());
        }
    }

    let price = match price {
        LimitOrderPrice::At(price) => price,
        LimitOrderPrice::AmountOverAsk(extra) => bid_ask.ask_price + extra,
        LimitOrderPrice::AmountUnderBid(_) => {
            panic!("Bug: AmountUnderBid invalid for a sell order")
        }
    };
    let mut price = (price * 100.).round() / 100.; // Round to two decimal places

    if let Some(if_price_over) = if_price_over {
        if price <= if_price_over {
            let msg = format!(
                "Order declined because price, ${price}, is not greater than ${if_price_over}",
            );
            println!("{msg}");
            notifier.send(&format!("{exchange:?}: {msg}")).await;
            return Ok(());
        }
    }

    if let Some(price_floor) = price_floor {
        if price < price_floor {
            let msg =
                format!("Proposed price, ${price}, is beneath price floor. Adjusting upwards");
            price = price_floor;
            println!("{msg}");
            notifier.send(&format!("{exchange:?}: {msg}")).await;
        }
    }

    let order_lots = deposit_account.extract_lots(
        db,
        token.amount(amount),
        lot_selection_method,
        lot_numbers,
    )?;
    if if_price_over_basis {
        if let Some(basis) = order_lots.iter().find_map(|lot| {
            let basis = lot.acquisition.price();
            if Decimal::from_f64(price).unwrap() < basis {
                Some(basis)
            } else {
                None
            }
        }) {
            let msg =
                format!("Order declined because price, ${price}, is less than basis ${basis}",);
            println!("{msg}");
            notifier.send(&format!("{exchange:?}: {msg}")).await;
            return Ok(());
        }
    }

    if price < bid_ask.ask_price {
        return Err("Order price is less than ask price".into());
    }

    println!("Placing sell order for ◎{amount} at ${price}");
    println!("Lots");
    for lot in &order_lots {
        maybe_println_lot(
            deposit_account.token,
            lot,
            Decimal::from_f64(price),
            None,
            &mut 0.,
            &mut 0.,
            &mut 0.,
            &mut false,
            &mut 0.,
            None,
            true,
            true,
        )
        .await;
    }

    let order_id = exchange_client
        .place_order(&pair, OrderSide::Sell, price, amount)
        .await?;
    let msg = format!(
        "Order created: {}: {:?} ◎{} at ${}, id {}",
        pair,
        OrderSide::Sell,
        amount,
        price,
        order_id,
    );
    db.open_order(
        OrderSide::Sell,
        deposit_account,
        exchange,
        pair,
        price,
        order_id,
        order_lots,
        None,
    )?;
    println!("{msg}");
    notifier.send(&format!("{exchange:?}: {msg}")).await;
    Ok(())
}

fn println_jup_quote(from_token: MaybeToken, to_token: MaybeToken, quote: &jup_ag::Quote) {
    let route = quote
        .route_plan
        .iter()
        .map(|route_plan| route_plan.swap_info.label.clone().unwrap_or_default())
        .join(", ");
    println!(
        "Swap {}{} for {}{} (min: {}{}) via {}",
        from_token.symbol(),
        from_token.ui_amount(quote.in_amount),
        to_token.symbol(),
        to_token.ui_amount(quote.out_amount),
        to_token.symbol(),
        to_token.ui_amount(quote.other_amount_threshold),
        route,
    );
}

async fn process_jup_quote(
    from_token: MaybeToken,
    to_token: MaybeToken,
    ui_amount: f64,
    slippage_bps: u64,
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

    println_jup_quote(from_token, to_token, &quote);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_jup_swap<T: Signers>(
    db: &mut Db,
    rpc_clients: &RpcClients,
    address: Pubkey,
    from_token: MaybeToken,
    to_token: MaybeToken,
    ui_amount: Option<f64>,
    slippage_bps: u64,
    lot_selection_method: LotSelectionMethod,
    signers: T,
    existing_signature: Option<Signature>,
    if_from_balance_exceeds: Option<u64>,
    for_no_less_than: Option<f64>,
    max_coingecko_value_percentage_loss: f64,
    priority_fee: PriorityFee,
    notifier: &Notifier,
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

        let swap_prefix = format!("Swap {}->{}", from_token, to_token);

        if let Some(if_from_balance_exceeds) = if_from_balance_exceeds {
            if from_account.last_update_balance < if_from_balance_exceeds {
                println!(
                    "{swap_prefix} declined because {} ({}) balance is less than {}{}",
                    address,
                    from_token.name(),
                    from_token.symbol(),
                    from_token.ui_amount(if_from_balance_exceeds)
                );
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

        println!("Fetching best {from_token}->{to_token} quote...");
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

        println_jup_quote(from_token, to_token, &quote);

        let from_value =
            from_token_price * Decimal::from_f64(from_token.ui_amount(quote.in_amount)).unwrap();
        let min_to_value = to_token_price
            * Decimal::from_f64(to_token.ui_amount(quote.other_amount_threshold)).unwrap();

        let swap_value_percentage_loss = Decimal::from_usize(100).unwrap()
            - min_to_value / from_value * Decimal::from_usize(100).unwrap();

        println!("Coingecko value loss: {swap_value_percentage_loss:.2}%");
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
                println!("{msg}");
                notifier.send(&msg).await;
                return Ok(());
            }
        }

        println!("Generating {swap_prefix} Transaction...");
        let mut swap_request = jup_ag::SwapRequest::new(address, quote.clone());
        swap_request.wrap_and_unwrap_sol = Some(from_token.is_sol() || to_token.is_sol());

        if let Some(lamports) = priority_fee.exact_lamports() {
            swap_request.prioritization_fee_lamports =
                jup_ag::PrioritizationFeeLamports::Exact { lamports };
        }

        let mut transaction = jup_ag::swap(swap_request).await?.swap_transaction;

        {
            let mut transaction_compute_budget = sys::priority_fee::ComputeBudget::default();

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
            println!(
                "Swap priority fee: {}",
                Sol(transaction_compute_budget.priority_fee_lamports())
            );
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

async fn process_sync_swaps(
    db: &mut Db,
    rpc_client: &RpcClient,
    notifier: &Notifier,
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
                    println!("Pending {swap} confirmed: {signature}");
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
                    println!("{msg}");
                } else {
                    println!("Pending {swap} failed with {result:?}: {signature}");
                    db.cancel_swap(signature)?;
                }
            }
            None => {
                if block_height > last_valid_block_height {
                    println!("Pending {swap} cancelled: {signature}");
                    db.cancel_swap(signature)?;
                } else {
                    println!(
                        "{} pending for at most {} blocks: {}",
                        swap,
                        last_valid_block_height.saturating_sub(block_height),
                        signature
                    );
                }
            }
        }
    }

    Ok(())
}

struct LiquidityTokenInfo {
    liquidity_token: MaybeToken,
    current_liquidity_token_rate: Decimal,
    current_apr: Option<f64>,
}

fn liquidity_token_ui_amount(
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
async fn maybe_println_lot(
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
                "value: ${}{}",
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
        "{:>5}. {} | {:>20} at ${:<6} | {:<35} | income: ${:<11} | {} gain: ${:<14}{} {}",
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
        println!("{msg}");
    }
    // }
}

fn format_disposed_lot(
    disposed_lot: &DisposedLot,
    total_income: &mut f64,
    total_cap_gain: &mut f64,
    long_term_cap_gain: &mut bool,
    total_current_value: &mut f64,
    verbose: bool,
) -> String {
    #![allow(clippy::to_string_in_format_args)]
    let cap_gain = disposed_lot
        .lot
        .cap_gain(disposed_lot.token, disposed_lot.price());
    let income = disposed_lot.lot.income(disposed_lot.token);

    *long_term_cap_gain =
        is_long_term_cap_gain(disposed_lot.lot.acquisition.when, Some(disposed_lot.when));
    *total_income += income;
    *total_current_value += income + cap_gain;
    *total_cap_gain += cap_gain;

    let description = if verbose {
        format!(
            "| {} | {}",
            disposed_lot.lot.acquisition.kind, disposed_lot.kind
        )
    } else {
        String::new()
    };

    format!(
        "{:>5}. {} | {:<7} | {:<17} at ${:<6} | income: ${:<11} | sold {} at ${:6} | {} gain: ${:<14} {}",
        disposed_lot.lot.lot_number,
        disposed_lot.lot.acquisition.when,
        disposed_lot.token.to_string(),
        disposed_lot.token.format_amount(disposed_lot.lot.amount),
        f64::try_from(disposed_lot.lot.acquisition.price()).unwrap().separated_string_with_fixed_place(2),
        income.separated_string_with_fixed_place(2),
        disposed_lot.when,
        f64::try_from(disposed_lot.price()).unwrap().separated_string_with_fixed_place(2),
        if *long_term_cap_gain {
            " long"
        } else {
            "short"
        },
        cap_gain.separated_string_with_fixed_place(2),
        description,
    )
}

#[allow(clippy::too_many_arguments)]
async fn process_account_add(
    db: &mut Db,
    rpc_client: &RpcClient,
    address: Pubkey,
    token: MaybeToken,
    description: String,
    when: Option<NaiveDate>,
    price: Option<f64>,
    income: bool,
    signature: Option<Signature>,
    no_sync: bool,
    ui_amount: Option<f64>,
    ui_negative_amount: Option<f64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (when, amount, last_update_epoch, kind) = match signature {
        Some(signature) => {
            assert!(ui_amount.is_none()); // argument parsing should have asserted this already
            assert!(ui_negative_amount.is_none()); // argument parsing should have asserted this already

            let (address, address_is_token) = match token.token() {
                Some(token) => (token.ata(&address), true),
                None => (address, false),
            };

            let GetTransactionAddrssBalanceChange {
                post_amount,
                slot,
                when: block_time,
                ..
            } = get_transaction_balance_change(rpc_client, &signature, &address, address_is_token)?;

            let when = block_time.map(|dt| dt.date()).or_else(|| {
                println!(
                    "Block time not available for slot {slot}, using `--when` argument instead"
                );
                when
            });

            let epoch_schdule = rpc_client.get_epoch_schedule()?;
            let last_update_epoch = epoch_schdule
                .get_epoch_and_slot_index(slot)
                .0
                .saturating_sub(1);

            (
                when,
                post_amount,
                last_update_epoch,
                LotAcquistionKind::Transaction { slot, signature },
            )
        }
        None => {
            let amount = match ui_amount {
                Some(ui_amount) => {
                    assert!(ui_negative_amount.is_none()); // argument parsing should have asserted this already
                    token.amount(ui_amount)
                }
                None => {
                    let amount = token.balance(rpc_client, &address)?;
                    match ui_negative_amount {
                        Some(ui_negative_amount) => {
                            amount.saturating_sub(token.amount(ui_negative_amount))
                        }
                        None => amount,
                    }
                }
            };

            let last_update_epoch = rpc_client.get_epoch_info()?.epoch.saturating_sub(1);
            (
                when,
                amount,
                last_update_epoch,
                if income {
                    LotAcquistionKind::NotAvailable
                } else {
                    LotAcquistionKind::Fiat
                },
            )
        }
    };

    println!("Adding {address} (token: {token})");

    let current_price = token.get_current_price(rpc_client).await?;
    let decimal_price = match price {
        Some(price) => Decimal::from_f64(price).unwrap(),
        None => match when {
            Some(when) => token.get_historical_price(rpc_client, when).await?,
            None => current_price,
        },
    };

    let mut lots = vec![];
    if amount > 0 {
        let lot = Lot {
            lot_number: db.next_lot_number(),
            acquisition: LotAcquistion::new(when.unwrap_or_else(today), decimal_price, kind),
            amount,
        };
        maybe_println_lot(
            token,
            &lot,
            Some(current_price),
            None,
            &mut 0.,
            &mut 0.,
            &mut 0.,
            &mut false,
            &mut 0.,
            None,
            true,
            true,
        )
        .await;

        lots.push(lot);
    }

    let account = TrackedAccount {
        address,
        token,
        description,
        last_update_epoch,
        last_update_balance: amount,
        lots,
        no_sync: Some(no_sync),
    };
    db.add_account(account)?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_account_dispose(
    db: &mut Db,
    rpc_client: &RpcClient,
    address: Pubkey,
    token: MaybeToken,
    ui_amount: f64,
    description: String,
    when: Option<NaiveDate>,
    price: Option<f64>,
    lot_selection_method: LotSelectionMethod,
    lot_numbers: Option<HashSet<usize>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let price = match price {
        Some(price) => Decimal::from_f64(price).unwrap(),
        None => match when {
            Some(when) => token.get_historical_price(rpc_client, when).await?,
            None => token.get_current_price(rpc_client).await?,
        },
    };

    let disposed_lots = db.record_disposal(
        address,
        token,
        token.amount(ui_amount),
        description,
        when.unwrap_or_else(today),
        price,
        lot_selection_method,
        lot_numbers,
    )?;
    if !disposed_lots.is_empty() {
        println!("Disposed Lots:");
        for disposed_lot in disposed_lots {
            println!(
                "{}",
                format_disposed_lot(&disposed_lot, &mut 0., &mut 0., &mut false, &mut 0., true)
            );
        }
        println!();
    }
    Ok(())
}

#[derive(Default, Debug, PartialEq)]
struct RealizedGain {
    income: f64,
    short_term_cap_gain: f64,
    long_term_cap_gain: f64,
    basis: f64,
}

#[derive(Default)]
struct AnnualRealizedGain {
    by_quarter: [RealizedGain; 4],
    by_payment_period: [RealizedGain; 4],
}

impl AnnualRealizedGain {
    const MONTH_TO_PAYMENT_PERIOD: [usize; 12] = [0, 0, 0, 1, 1, 2, 2, 2, 3, 3, 3, 3];

    fn record_income(&mut self, month: usize, income: f64) {
        self.by_quarter[month / 3].income += income;
        self.by_payment_period[Self::MONTH_TO_PAYMENT_PERIOD[month]].income += income;
    }

    fn record_short_term_cap_gain(&mut self, month: usize, cap_gain: f64) {
        self.by_quarter[month / 3].short_term_cap_gain += cap_gain;
        self.by_payment_period[Self::MONTH_TO_PAYMENT_PERIOD[month]].short_term_cap_gain +=
            cap_gain;
    }

    fn record_long_term_cap_gain(&mut self, month: usize, cap_gain: f64) {
        self.by_quarter[month / 3].long_term_cap_gain += cap_gain;
        self.by_payment_period[Self::MONTH_TO_PAYMENT_PERIOD[month]].long_term_cap_gain += cap_gain;
    }
}

async fn process_account_cost_basis(
    db: &Db,
    when: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut held_tokens =
        BTreeMap::<MaybeToken, Vec<(/*amount: */ u64, /*price: */ Decimal)>>::default();

    println!("Average Cost Basis on {when}");
    for disposed_lot in db.disposed_lots() {
        if disposed_lot.lot.acquisition.when > when || disposed_lot.when < when {
            continue;
        }
        held_tokens
            .entry(disposed_lot.token)
            .or_insert_with(Vec::new)
            .push((
                disposed_lot.lot.amount,
                disposed_lot.lot.acquisition.price(),
            ));
    }

    for account in db.get_accounts() {
        let held_token = held_tokens.entry(account.token).or_insert_with(Vec::new);
        for lot in account.lots {
            if lot.acquisition.when <= when {
                held_token.push((lot.amount, lot.acquisition.price()));
            }
        }
    }

    // Merge wSOL lots into SOL
    if let Some(mut lots) = held_tokens.remove(&Token::wSOL.into()) {
        held_tokens
            .entry(MaybeToken::SOL())
            .or_insert_with(Vec::new)
            .append(&mut lots);
    }

    for (token, lots) in held_tokens {
        if lots.is_empty() || token.fiat_fungible() {
            continue;
        }

        let mut total_amount = 0;
        let mut total_price = Decimal::default();

        for (amount, price) in lots {
            total_amount += amount;
            total_price += Decimal::from_f64(token.ui_amount(amount)).unwrap() * price;
        }
        println!(
            "  {:>7}: {:<20} at ${} ; ${:.2} per {}",
            token.to_string(),
            token.format_amount(total_amount),
            TryInto::<f64>::try_into(total_price)
                .unwrap()
                .separated_string_with_fixed_place(2),
            total_price / Decimal::from_f64(token.ui_amount(total_amount)).unwrap(),
            token.name()
        );
    }
    Ok(())
}

fn print_current_holdings(
    held_tokens: &BTreeMap<
        MaybeToken,
        (
            /*price*/ Option<Decimal>,
            /*amount*/ u64,
            RealizedGain,
        ),
    >,
    tax_rate: Option<&TaxRate>,
) {
    println!("Current Holdings");
    let mut held_tokens = held_tokens
        .iter()
        .map(
            |(held_token, (current_token_price, total_held_amount, unrealized_gain))| {
                let total_value = current_token_price.map(|current_token_price| {
                    f64::try_from(
                        Decimal::from_f64(held_token.ui_amount(*total_held_amount)).unwrap()
                            * current_token_price,
                    )
                    .unwrap()
                });

                (
                    held_token,
                    total_value,
                    current_token_price,
                    total_held_amount,
                    unrealized_gain,
                )
            },
        )
        .collect::<Vec<_>>();

    // Order current holdings by `total_value`
    held_tokens.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    for (held_token, total_value, current_token_price, total_held_amount, unrealized_gain) in
        held_tokens
    {
        if *total_held_amount == 0 {
            continue;
        }

        let estimated_tax = tax_rate
            .and_then(|tax_rate| {
                let tax = unrealized_gain.short_term_cap_gain * tax_rate.short_term_gain
                    + unrealized_gain.long_term_cap_gain * tax_rate.long_term_gain;

                if tax > 0. {
                    Some(format!(
                        "; ${} estimated tax",
                        tax.separated_string_with_fixed_place(2)
                    ))
                } else {
                    None
                }
            })
            .unwrap_or_default();

        if held_token.fiat_fungible() {
            println!(
                "  {:<7}       {:<20}",
                held_token.to_string(),
                held_token.format_amount(*total_held_amount)
            );
        } else {
            println!(
                "  {:<7}       {:<20} [{}; ${:>4} per {:>4}{}]",
                held_token.to_string(),
                held_token.format_amount(*total_held_amount),
                total_value
                    .map(|tv| {
                        format!(
                            "${:14} ({:>8}%)",
                            tv.separated_string_with_fixed_place(2),
                            ((tv - unrealized_gain.basis) / unrealized_gain.basis * 100.)
                                .separated_string_with_fixed_place(2)
                        )
                    })
                    .unwrap_or_else(|| "?".into()),
                current_token_price
                    .map(|current_token_price| f64::try_from(current_token_price)
                        .unwrap()
                        .separated_string_with_fixed_place(3))
                    .unwrap_or_else(|| "?".into()),
                held_token,
                estimated_tax,
            );
        }
    }
    println!();
}

async fn process_account_list(
    db: &Db,
    rpc_client: &RpcClient,
    account_filter: Option<Pubkey>,
    show_all_lots: bool,
    summary_only: bool,
    notifier: &Notifier,
    verbose: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut annual_realized_gains = BTreeMap::<usize, AnnualRealizedGain>::default();
    let mut held_tokens = BTreeMap::<
        MaybeToken,
        (
            /*price*/ Option<Decimal>,
            /*amount*/ u64,
            RealizedGain,
        ),
    >::default();

    // hacky: display a unified rate if the long and short term rate is equal
    let unified_tax_rate = db
        .get_tax_rate()
        .map(|tax_rate| tax_rate.short_term_gain - tax_rate.long_term_gain <= f64::EPSILON)
        .unwrap_or(false);

    let mut accounts = db.get_accounts();
    accounts.sort_by(|a, b| {
        let mut result = a.last_update_balance.cmp(&b.last_update_balance);
        if result == std::cmp::Ordering::Equal {
            result = a.address.cmp(&b.address);
        }
        if result == std::cmp::Ordering::Equal {
            result = a.description.cmp(&b.description);
        }
        result
    });
    if accounts.is_empty() {
        println!("No accounts");
    } else {
        let mut total_income = 0.;
        let mut total_unrealized_short_term_gain = 0.;
        let mut total_unrealized_long_term_gain = 0.;
        let mut total_current_basis = 0.;
        let mut total_current_fiat_value = 0.;
        let mut total_current_value = 0.;

        let open_orders = db.open_orders(None, None);

        for account in accounts {
            if let Some(ref account_filter) = account_filter {
                if account.address != *account_filter {
                    continue;
                }
            }

            if let std::collections::btree_map::Entry::Vacant(e) = held_tokens.entry(account.token)
            {
                e.insert((
                    account.token.get_current_price(rpc_client).await.ok(),
                    0,
                    RealizedGain::default(),
                ));
            }

            let held_token = held_tokens.get_mut(&account.token).unwrap();
            let current_token_price = held_token.0;
            held_token.1 += account.last_update_balance;

            let ui_amount = account.token.ui_amount(account.last_update_balance);

            let liquidity_token_info =
                if let Some(liquidity_token) = account.token.liquidity_token() {
                    if let Ok(current_liquidity_token_rate) = account
                        .token
                        .get_current_liquidity_token_rate(rpc_client)
                        .await
                    {
                        Some(LiquidityTokenInfo {
                            liquidity_token,
                            current_liquidity_token_rate,
                            current_apr: None,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                };

            let (liquidity_ui_amount, _) =
                liquidity_token_ui_amount(None, ui_amount, liquidity_token_info.as_ref(), true);
            let msg = format!(
                "{} ({}): {}{}{} - {}",
                account.address,
                account.token,
                account.token.symbol(),
                ui_amount.separated_string_with_fixed_place(2),
                liquidity_ui_amount,
                account.description
            );
            println!("{msg}");
            if ui_amount > 0.01 {
                notifier.send(&msg).await;
            }
            account.assert_lot_balance();

            if summary_only {
                if !account.lots.is_empty() {
                    let mut account_basis = 0.;
                    let mut account_value = 0.;
                    for lot in account.lots.iter() {
                        let value = current_token_price.map(|price| {
                            f64::try_from(
                                Decimal::from_f64(account.token.ui_amount(lot.amount)).unwrap()
                                    * price,
                            )
                            .unwrap()
                        });
                        account_basis += lot.basis(account.token);
                        account_value += value.unwrap_or_default();
                    }
                    held_token.2.basis += account_basis;
                    total_current_basis += account_basis;
                    total_current_value += account_value;
                }
                continue;
            }

            let open_orders = open_orders
                .iter()
                .filter(|oo| oo.deposit_address == account.address && oo.token == account.token)
                .collect::<Vec<_>>();

            if !account.lots.is_empty() || !open_orders.is_empty() {
                let mut lots = account.lots.iter().collect::<Vec<_>>();
                lots.sort_by_key(|lot| lot.acquisition.when);

                let mut account_basis = 0.;
                let mut account_income = 0.;
                let mut account_current_value = 0.;
                let mut account_unrealized_short_term_gain = 0.;
                let mut account_unrealized_long_term_gain = 0.;

                if !show_all_lots && lots.len() > 5 {
                    println!("  ...");
                }

                for (i, lot) in lots.iter().enumerate() {
                    let mut account_unrealized_gain = 0.;
                    let mut long_term_cap_gain = false;

                    maybe_println_lot(
                        account.token,
                        lot,
                        current_token_price,
                        liquidity_token_info.as_ref(),
                        &mut account_basis,
                        &mut account_income,
                        &mut account_unrealized_gain,
                        &mut long_term_cap_gain,
                        &mut account_current_value,
                        None,
                        verbose,
                        if show_all_lots {
                            true
                        } else {
                            lots.len() < 5 || (i > lots.len().saturating_sub(5))
                        },
                    )
                    .await;

                    annual_realized_gains
                        .entry(lot.acquisition.when.year() as usize)
                        .or_default()
                        .record_income(
                            lot.acquisition.when.month0() as usize,
                            lot.income(account.token),
                        );

                    if long_term_cap_gain {
                        account_unrealized_long_term_gain += account_unrealized_gain;
                    } else {
                        account_unrealized_short_term_gain += account_unrealized_gain;
                    }
                }

                for open_order in open_orders {
                    let mut lots = open_order.lots.iter().collect::<Vec<_>>();
                    lots.sort_by_key(|lot| lot.acquisition.when);
                    let ui_amount = open_order.ui_amount.unwrap_or_else(|| {
                        account
                            .token
                            .ui_amount(lots.iter().map(|lot| lot.amount).sum::<u64>())
                    });
                    println!(
                        " [Open {}: {} {} at ${} | id {} created {}]",
                        open_order.pair,
                        format_order_side(open_order.side),
                        account.token.format_ui_amount(ui_amount),
                        open_order.price,
                        open_order.order_id,
                        HumanTime::from(open_order.creation_time),
                    );
                    for lot in lots {
                        let mut account_unrealized_gain = 0.;
                        let mut long_term_cap_gain = false;
                        maybe_println_lot(
                            account.token,
                            lot,
                            current_token_price,
                            liquidity_token_info.as_ref(),
                            &mut account_basis,
                            &mut account_income,
                            &mut account_unrealized_gain,
                            &mut long_term_cap_gain,
                            &mut account_current_value,
                            None,
                            verbose,
                            true,
                        )
                        .await;

                        annual_realized_gains
                            .entry(lot.acquisition.when.year() as usize)
                            .or_default()
                            .record_income(
                                lot.acquisition.when.month0() as usize,
                                lot.income(account.token),
                            );

                        if long_term_cap_gain {
                            account_unrealized_long_term_gain += account_unrealized_gain;
                        } else {
                            account_unrealized_short_term_gain += account_unrealized_gain;
                        }
                    }
                }

                println!(
                    "    Value: ${}{}",
                    account_current_value.separated_string_with_fixed_place(2),
                    if account.token.fiat_fungible() {
                        "".into()
                    } else {
                        format!(
                            " ({}%), {}{}",
                            ((account_current_value - account_basis) / account_basis * 100.)
                                .separated_string_with_fixed_place(2),
                            if account_income > 0. {
                                format!(
                                    "income: ${}, ",
                                    account_income.separated_string_with_fixed_place(2)
                                )
                            } else {
                                "".into()
                            },
                            if unified_tax_rate {
                                format!(
                                    "unrealized cap gain: ${}",
                                    (account_unrealized_short_term_gain
                                        + account_unrealized_long_term_gain)
                                        .separated_string_with_fixed_place(2)
                                )
                            } else {
                                format!("unrealized short-term cap gain: ${}, unrealized long-term cap gain: ${}",
                                    account_unrealized_short_term_gain.separated_string_with_fixed_place(2),
                                    account_unrealized_long_term_gain.separated_string_with_fixed_place(2)
                                )
                            }
                        )
                    }
                );

                total_unrealized_short_term_gain += account_unrealized_short_term_gain;
                total_unrealized_long_term_gain += account_unrealized_long_term_gain;
                total_income += account_income;
                total_current_value += account_current_value;
                if account.token.fiat_fungible() {
                    total_current_fiat_value += account_current_value;
                } else {
                    total_current_basis += account_basis;
                }

                held_token.2.short_term_cap_gain += account_unrealized_short_term_gain;
                held_token.2.long_term_cap_gain += account_unrealized_long_term_gain;
                held_token.2.basis += account_basis;
            } else {
                println!("  No lots");
            }
            println!();
        }

        if summary_only {
            print_current_holdings(&held_tokens, db.get_tax_rate());
            println!(
                "Current Value:       ${} ({}%)",
                total_current_value.separated_string_with_fixed_place(2),
                (((total_current_value - total_current_fiat_value) - total_current_basis)
                    / total_current_basis
                    * 100.)
                    .separated_string_with_fixed_place(2),
            );
        }
        if account_filter.is_some() || summary_only {
            return Ok(());
        }

        let mut disposed_lots = db.disposed_lots();
        disposed_lots.sort_by_key(|lot| lot.when);
        if !disposed_lots.is_empty() {
            println!("Disposed ({} lots):", disposed_lots.len());

            let mut disposed_income = 0.;
            let mut disposed_short_term_cap_gain = 0.;
            let mut disposed_long_term_cap_gain = 0.;
            let mut disposed_value = 0.;

            for (i, disposed_lot) in disposed_lots.iter().enumerate() {
                let mut long_term_cap_gain = false;
                let mut disposed_cap_gain = 0.;
                let msg = format_disposed_lot(
                    disposed_lot,
                    &mut disposed_income,
                    &mut disposed_cap_gain,
                    &mut long_term_cap_gain,
                    &mut disposed_value,
                    verbose,
                );

                if show_all_lots {
                    println!("{msg}");
                } else {
                    if disposed_lots.len() > 5 && i == disposed_lots.len().saturating_sub(5) {
                        println!("...");
                    }
                    if i > disposed_lots.len().saturating_sub(5) {
                        println!("{msg}");
                    }
                }

                annual_realized_gains
                    .entry(disposed_lot.lot.acquisition.when.year() as usize)
                    .or_default()
                    .record_income(
                        disposed_lot.lot.acquisition.when.month0() as usize,
                        disposed_lot.lot.income(disposed_lot.token),
                    );

                let annual_realized_gain = annual_realized_gains
                    .entry(disposed_lot.when.year() as usize)
                    .or_default();

                if long_term_cap_gain {
                    disposed_long_term_cap_gain += disposed_cap_gain;
                    annual_realized_gain.record_long_term_cap_gain(
                        disposed_lot.when.month0() as usize,
                        disposed_cap_gain,
                    );
                } else {
                    disposed_short_term_cap_gain += disposed_cap_gain;
                    annual_realized_gain.record_short_term_cap_gain(
                        disposed_lot.when.month0() as usize,
                        disposed_cap_gain,
                    );
                }
            }
            println!(
                "    Disposed value: ${} ({}{})",
                disposed_value.separated_string_with_fixed_place(2),
                if disposed_income > 0. {
                    format!(
                        "income: ${}, ",
                        disposed_income.separated_string_with_fixed_place(2)
                    )
                } else {
                    "".into()
                },
                if unified_tax_rate {
                    format!(
                        "cap gain: ${}",
                        (disposed_short_term_cap_gain + disposed_long_term_cap_gain)
                            .separated_string_with_fixed_place(2)
                    )
                } else {
                    format!(
                        "short-term cap gain: ${}, long-term cap gain: ${}",
                        disposed_short_term_cap_gain.separated_string_with_fixed_place(2),
                        disposed_long_term_cap_gain.separated_string_with_fixed_place(2)
                    )
                }
            );
            println!();
        }

        if let Some(sweep_stake_account) = db.get_sweep_stake_account() {
            println!("Sweep stake account: {}", sweep_stake_account.address);
            println!(
                "Stake authority: {}",
                sweep_stake_account.stake_authority.display()
            );
            println!();
        }

        let tax_rate = db.get_tax_rate();
        println!("Realized Gains");
        if unified_tax_rate {
            println!("  Year    | Income          |       Cap gain | Estimated Tax ");
        } else {
            println!(
                "  Year    | Income          | Short-term gain | Long-term gain | Estimated Tax "
            );
        }
        for (year, annual_realized_gain) in annual_realized_gains {
            let (symbol, realized_gains) = {
                ('P', annual_realized_gain.by_payment_period)
                // TODO: Add user option to restore `by_quarter` display
                //('Q', annual_realized_gains.by_quarter)
            };
            for (q, realized_gain) in realized_gains.iter().enumerate() {
                if *realized_gain != RealizedGain::default() {
                    let tax = if let Some(tax_rate) = tax_rate {
                        let tax = [
                            realized_gain.income * tax_rate.income,
                            realized_gain.short_term_cap_gain * tax_rate.short_term_gain
                                + realized_gain.long_term_cap_gain * tax_rate.long_term_gain,
                        ]
                        .into_iter()
                        .map(|x| x.max(0.))
                        .sum::<f64>();

                        if tax > 0. {
                            format!("${}", tax.separated_string_with_fixed_place(2))
                        } else {
                            String::new()
                        }
                    } else {
                        "-".into()
                    };

                    println!(
                        "  {} {}{} | ${:14} | {}| {}",
                        year,
                        symbol,
                        q + 1,
                        realized_gain.income.separated_string_with_fixed_place(2),
                        if unified_tax_rate {
                            format!(
                                "${:14}",
                                (realized_gain.short_term_cap_gain
                                    + realized_gain.long_term_cap_gain)
                                    .separated_string_with_fixed_place(2)
                            )
                        } else {
                            format!(
                                "${:14} | ${:14}",
                                realized_gain
                                    .short_term_cap_gain
                                    .separated_string_with_fixed_place(2),
                                realized_gain
                                    .long_term_cap_gain
                                    .separated_string_with_fixed_place(2)
                            )
                        },
                        tax
                    );
                }
            }
        }
        println!();

        print_current_holdings(&held_tokens, tax_rate);

        println!("Summary");
        println!(
            "  Current Value:       ${} ({}%)",
            total_current_value.separated_string_with_fixed_place(2),
            (((total_current_value - total_current_fiat_value) - total_current_basis)
                / total_current_basis
                * 100.)
                .separated_string_with_fixed_place(2),
        );
        if total_income > 0. {
            println!(
                "  Income:              ${} (realized)",
                total_income.separated_string_with_fixed_place(2)
            );
        }
        if unified_tax_rate {
            println!(
                "  Cap gain:            ${} (unrealized)",
                (total_unrealized_short_term_gain + total_unrealized_long_term_gain)
                    .separated_string_with_fixed_place(2)
            );
        } else {
            println!(
                "  Short-term cap gain: ${} (unrealized)",
                total_unrealized_short_term_gain.separated_string_with_fixed_place(2)
            );
            println!(
                "  Long-term cap gain:  ${} (unrealized)",
                total_unrealized_long_term_gain.separated_string_with_fixed_place(2)
            );
        }

        let pending_deposits = db.pending_deposits(None).len();
        let pending_withdrawals = db.pending_withdrawals(None).len();
        let pending_transfers = db.pending_transfers().len();
        let pending_swaps = db.pending_swaps().len();

        if pending_deposits + pending_withdrawals + pending_transfers + pending_swaps > 0 {
            println!();
        }
        if pending_deposits > 0 {
            println!("  !! Pending deposits: {pending_deposits}");
        }
        if pending_withdrawals > 0 {
            println!("  !! Pending withdrawals: {pending_withdrawals}");
        }
        if pending_transfers > 0 {
            println!("  !! Pending transfers: {pending_transfers}");
        }
        if pending_swaps > 0 {
            println!("  !! Pending swaps: {pending_swaps}");
        }
    }

    Ok(())
}

async fn process_account_xls(
    db: &Db,
    outfile: &str,
    filter_by_year: Option<i32>,
) -> Result<(), Box<dyn std::error::Error>> {
    use simple_excel_writer::*;

    let mut workbook = Workbook::create(outfile);

    let mut sheet = workbook.create_sheet(&match filter_by_year {
        Some(year) => format!("Disposed in {year}"),
        None => "Disposed".into(),
    });
    sheet.add_column(Column { width: 12. });
    sheet.add_column(Column { width: 15. });
    sheet.add_column(Column { width: 12. });
    sheet.add_column(Column { width: 12. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 40. });
    sheet.add_column(Column { width: 12. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 40. });

    let mut disposed_lots = db.disposed_lots();
    disposed_lots.sort_by_key(|lot| lot.when);

    if let Some(year) = filter_by_year {
        // Exclude disposed lots that were neither acquired nor disposed of in the filter year
        disposed_lots.retain(|disposed_lot| {
            (disposed_lot.lot.acquisition.when.year() == year
                && disposed_lot.lot.income(disposed_lot.token) > 0.)
                || disposed_lot.when.year() == year
        })
    }

    workbook.write_sheet(&mut sheet, |sheet_writer| {
        sheet_writer.append_row(row![
            "Token",
            "Amount",
            "Income (USD)",
            "Acq. Date",
            "Acq. Price (USD)",
            "Acquisition Description",
            "Cap Gain (USD)",
            "Cap Gain Type",
            "Sale Date",
            "Sale Price (USD)",
            "Fee (USD)",
            "Sale Description"
        ])?;

        for disposed_lot in disposed_lots {
            let long_term_cap_gain =
                is_long_term_cap_gain(disposed_lot.lot.acquisition.when, Some(disposed_lot.when));

            let mut income = disposed_lot.lot.income(disposed_lot.token);
            if let Some(year) = filter_by_year {
                if disposed_lot.lot.acquisition.when.year() != year {
                    income = 0. // Exclude income from other years
                }
            }

            sheet_writer.append_row(row![
                disposed_lot.token.to_string(),
                disposed_lot.token.ui_amount(disposed_lot.lot.amount),
                income,
                disposed_lot.lot.acquisition.when.to_string(),
                disposed_lot.lot.acquisition.price().to_string(),
                disposed_lot.lot.acquisition.kind.to_string(),
                disposed_lot
                    .lot
                    .cap_gain(disposed_lot.token, disposed_lot.price()),
                if long_term_cap_gain { "Long" } else { "Short" },
                disposed_lot.when.to_string(),
                disposed_lot.price().to_string(),
                disposed_lot
                    .kind
                    .fee()
                    .map(|(amount, currency)| {
                        assert_eq!(currency, "USD");
                        *amount
                    })
                    .unwrap_or_default(),
                disposed_lot.kind.to_string()
            ])?;
        }
        Ok(())
    })?;

    let mut current_holdings_rows = vec![];
    let mut current_holdings_by_year_rows = vec![];

    #[derive(Clone)]
    enum R {
        Number(f64),
        Text(String),
    }

    impl ToCellValue for R {
        fn to_cell_value(&self) -> CellValue {
            match self {
                R::Number(x) => x.to_cell_value(),
                R::Text(x) => x.to_cell_value(),
            }
        }
    }

    for account in db.get_accounts() {
        for lot in account.lots.iter() {
            let row = (
                lot.acquisition.when,
                vec![
                    R::Text(account.token.to_string()),
                    R::Number(account.token.ui_amount(lot.amount)),
                    R::Number(lot.income(account.token)),
                    R::Text(lot.acquisition.when.to_string()),
                    R::Text(lot.acquisition.price().to_string()),
                    R::Text(lot.acquisition.kind.to_string()),
                    R::Text(account.description.clone()),
                    R::Text(account.address.to_string()),
                ],
            );
            current_holdings_rows.push(row.clone());
            if let Some(year) = filter_by_year {
                if lot.acquisition.when.year() == year {
                    current_holdings_by_year_rows.push(row);
                    continue;
                }
            }
        }
    }

    for open_order in db.open_orders(None, Some(OrderSide::Sell)) {
        for lot in open_order.lots.iter() {
            let row = (
                lot.acquisition.when,
                vec![
                    R::Text(open_order.token.to_string()),
                    R::Number(open_order.token.ui_amount(lot.amount)),
                    R::Number(lot.income(open_order.token)),
                    R::Text(lot.acquisition.when.to_string()),
                    R::Text(lot.acquisition.price().to_string()),
                    R::Text(lot.acquisition.kind.to_string()),
                    R::Text(format!(
                        "Open Order: {:?} {}",
                        open_order.exchange, open_order.pair
                    )),
                    R::Text(open_order.deposit_address.to_string()),
                ],
            );
            current_holdings_rows.push(row.clone());
            if let Some(year) = filter_by_year {
                if lot.acquisition.when.year() == year {
                    current_holdings_by_year_rows.push(row);
                    continue;
                }
            }
        }
    }
    current_holdings_rows.sort_by_key(|row| row.0);
    current_holdings_by_year_rows.sort_by_key(|row| row.0);

    let mut write_holdings = |name: String, rows: Vec<(_, Vec<R>)>| {
        let mut sheet = workbook.create_sheet(&name);

        sheet.add_column(Column { width: 12. });
        sheet.add_column(Column { width: 15. });
        sheet.add_column(Column { width: 12. });
        sheet.add_column(Column { width: 12. });
        sheet.add_column(Column { width: 10. });
        sheet.add_column(Column { width: 40. });
        sheet.add_column(Column { width: 40. });
        sheet.add_column(Column { width: 50. });

        workbook.write_sheet(&mut sheet, |sheet_writer| {
            sheet_writer.append_row(row![
                "Token",
                "Amount",
                "Income (USD)",
                "Acq. Date",
                "Acq. Price (USD)",
                "Acquisition Description",
                "Account Description",
                "Account Address"
            ])?;

            for (_, row) in rows {
                sheet_writer.append_row(Row::from_iter(row.into_iter()))?;
            }

            Ok(())
        })
    };
    if let Some(year) = filter_by_year {
        write_holdings(
            format!("Holdings acquired in {year}"),
            current_holdings_by_year_rows,
        )?;
    }
    write_holdings("All Holdings".to_string(), current_holdings_rows)?;

    workbook.close()?;
    println!("Wrote {outfile}");

    Ok(())
}

async fn process_account_csv(
    db: &Db,
    outfile: &str,
    filter_by_year: Option<i32>,
) -> Result<(), Box<dyn std::error::Error>> {
    use csv::Writer;

    let mut wtr = Writer::from_path(outfile)?;
    let mut disposed_lots = db.disposed_lots();
    disposed_lots.sort_by_key(|lot| lot.when);

    if let Some(year) = filter_by_year {
        // Exclude disposed lots that were neither acquired nor disposed of in the filter year
        disposed_lots.retain(|disposed_lot| {
            (disposed_lot.lot.acquisition.when.year() == year
                && disposed_lot.lot.income(disposed_lot.token) > 0.)
                || disposed_lot.when.year() == year
        })
    }
    wtr.write_record([
        "Transaction Type",
        "Asset Name",
        "Amount",
        "Date Acquired",
        "Cost basis (USD)",
        "Date of Disposition",
        "Proceeds (USD)",
    ])?;

    for disposed_lot in disposed_lots {
        let cost = Decimal::from_u64(disposed_lot.lot.amount).unwrap()
            * disposed_lot.lot.acquisition.price()
            / Decimal::from_f64(1e9).unwrap();
        let proceedings = Decimal::from_u64(disposed_lot.lot.amount).unwrap()
            * disposed_lot.price()
            / Decimal::from_f64(1e9).unwrap();
        wtr.write_record(&[
            "Sell".to_string(),
            disposed_lot.token.to_string(),
            format!(
                "{:.9}",
                disposed_lot.token.ui_amount(disposed_lot.lot.amount)
            ),
            disposed_lot.lot.acquisition.when.to_string(),
            format!("{:.9}", cost),
            disposed_lot.when.to_string(),
            format!("{:.9}", proceedings),
        ])?;
    }

    wtr.flush()?;
    println!("Wrote {outfile}");

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_account_merge<T: Signers>(
    db: &mut Db,
    rpc_clients: &RpcClients,
    from_address: Pubkey,
    into_address: Pubkey,
    authority_address: Pubkey,
    signers: T,
    priority_fee: PriorityFee,
    existing_signature: Option<Signature>,
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

        println!("Merging {from_address} into {into_address}");
        if from_address != authority_address {
            println!("Authority address: {authority_address}");
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
        println!("Transaction signature: {signature}");

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
async fn process_account_sweep<T: Signers>(
    db: &mut Db,
    rpc_clients: &RpcClients,
    from_address: Pubkey,
    token: MaybeToken,
    retain_amount: u64,
    exact_amount: Option<u64>,
    no_sweep_ok: bool,
    from_authority_address: Pubkey,
    signers: T,
    to_address: Option<Pubkey>,
    notifier: &Notifier,
    priority_fee: PriorityFee,
    existing_signature: Option<Signature>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_client = rpc_clients.default();

    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

    let from_account = rpc_client
        .get_account_with_commitment(&from_address, rpc_client.commitment())?
        .value
        .ok_or_else(|| format!("Account, {from_address}, does not exist"))?;

    let from_tracked_account = db
        .get_account(from_address, token)
        .ok_or_else(|| format!("Account, {from_address}, is not tracked"))?;

    let authority_account = if from_address == from_authority_address {
        from_account.clone()
    } else {
        rpc_client
            .get_account_with_commitment(&from_authority_address, rpc_client.commitment())?
            .value
            .ok_or_else(|| format!("Authority account, {from_authority_address}, does not exist"))?
    };

    let (to_address, via_transitory_stake) = if let Some(to_address) = to_address {
        let _ = db
            .get_account(to_address, token)
            .ok_or_else(|| format!("Account {to_address} ({token}) does not exist"))?;
        (to_address, None)
    } else {
        if !token.is_sol() {
            return Err("--to <ADDRESS> must be provided for token sweeps".into());
        }

        if existing_signature.is_some() {
            return Err("--signature only supported for token sweeps".into());
        }

        let transitory_stake_account = Keypair::new();

        let sweep_stake_account = db
            .get_sweep_stake_account()
            .ok_or("Sweep stake account not configured")?;
        let sweep_stake_authority_keypair = read_keypair_file(&sweep_stake_account.stake_authority)
            .map_err(|err| {
                format!(
                    "Failed to read {}: {}",
                    sweep_stake_account.stake_authority.display(),
                    err
                )
            })?;

        (
            transitory_stake_account.pubkey(),
            Some((
                transitory_stake_account,
                sweep_stake_authority_keypair,
                sweep_stake_account.address,
            )),
        )
    };

    let apply_exact_amount = |amount: u64| -> Result<u64, Box<dyn std::error::Error>> {
        if let Some(exact_amount) = exact_amount {
            if exact_amount > amount {
                Err(format!("Account has insufficient balance: {}", from_address).into())
            } else {
                Ok(exact_amount)
            }
        } else {
            Ok(amount)
        }
    };

    let (mut instructions, sweep_amount) = if token.is_sol() {
        if from_account.lamports < from_tracked_account.last_update_balance {
            println!(
                "Warning: {}: On-chain account balance ({}) less than tracked balance ({})",
                from_address,
                token.ui_amount(from_account.lamports),
                token.ui_amount(from_tracked_account.last_update_balance)
            );
        }

        if from_account.owner == system_program::id() {
            let lamports = apply_exact_amount(if from_address == from_authority_address {
                let mut dummy_instructions =
                    vec![system_instruction::transfer(&from_address, &to_address, 0)];
                if let Some((transitory_stake_account, sweep_stake_authority_keypair, _)) =
                    via_transitory_stake.as_ref()
                {
                    dummy_instructions.append(&mut vec![
                        system_instruction::allocate(
                            &transitory_stake_account.pubkey(),
                            std::mem::size_of::<solana_sdk::stake::state::StakeStateV2>() as u64,
                        ),
                        system_instruction::assign(
                            &transitory_stake_account.pubkey(),
                            &solana_sdk::stake::program::id(),
                        ),
                        solana_sdk::stake::instruction::initialize(
                            &transitory_stake_account.pubkey(),
                            &Authorized::auto(&Pubkey::default()),
                            &solana_sdk::stake::state::Lockup::default(),
                        ),
                        solana_sdk::stake::instruction::delegate_stake(
                            &transitory_stake_account.pubkey(),
                            &sweep_stake_authority_keypair.pubkey(),
                            &Pubkey::default(),
                        ),
                    ]);
                }
                let dummy_message = Message::new_with_blockhash(
                    &dummy_instructions,
                    Some(&from_authority_address),
                    &recent_blockhash,
                );
                let fee = rpc_client.get_fee_for_message(&dummy_message)?;
                from_tracked_account
                    .last_update_balance
                    .saturating_sub(fee + retain_amount)
            } else {
                from_tracked_account
                    .last_update_balance
                    .saturating_sub(retain_amount)
            })?;

            (
                vec![system_instruction::transfer(
                    &from_address,
                    &to_address,
                    lamports,
                )],
                lamports,
            )
        } else if from_account.owner == solana_program::vote::program::id() {
            let minimum_balance = rpc_client.get_minimum_balance_for_rent_exemption(
                solana_program::vote::state::VoteState::size_of(),
            )?;

            let lamports = apply_exact_amount(
                from_tracked_account
                    .last_update_balance
                    .saturating_sub(minimum_balance + retain_amount),
            )?;

            (
                vec![solana_program::vote::instruction::withdraw(
                    &from_address,
                    &from_authority_address,
                    lamports,
                    &to_address,
                )],
                lamports,
            )
        } else if from_account.owner == solana_sdk::stake::program::id() {
            let lamports = apply_exact_amount(
                from_tracked_account
                    .last_update_balance
                    .saturating_sub(retain_amount),
            )?;

            (
                vec![solana_sdk::stake::instruction::withdraw(
                    &from_address,
                    &from_authority_address,
                    &to_address,
                    lamports,
                    None,
                )],
                lamports,
            )
        } else {
            return Err(format!("Unsupported `from` account owner: {}", from_account.owner).into());
        }
    } else {
        let token = token.token().unwrap();

        let amount = apply_exact_amount(
            from_tracked_account
                .last_update_balance
                .saturating_sub(retain_amount),
        )?;

        (
            vec![spl_token::instruction::transfer_checked(
                &token.program_id(),
                &token.ata(&from_address),
                &token.mint(),
                &token.ata(&to_address),
                &from_authority_address,
                &[],
                amount,
                token.decimals(),
            )
            .unwrap()],
            amount,
        )
    };

    if sweep_amount < token.amount(1.) {
        let msg = format!(
            "{} has less than {}1 to sweep ({})",
            from_address,
            token.symbol(),
            token.ui_amount(sweep_amount)
        );
        return if no_sweep_ok {
            println!("{msg}");
            Ok(())
        } else {
            Err(msg.into())
        };
    }

    println!("From address: {from_address}");
    if from_address != from_authority_address {
        println!("Authority address: {from_authority_address}");
    }
    println!("Destination address: {to_address}");
    println!(
        "Sweep amount: {}{}",
        token.symbol(),
        token.ui_amount(sweep_amount)
    );

    let msg = if let Some((
        transitory_stake_account,
        sweep_stake_authority_keypair,
        sweep_stake_address,
    )) = via_transitory_stake.as_ref()
    {
        assert!(existing_signature.is_none());
        assert_eq!(to_address, transitory_stake_account.pubkey());

        let (sweep_stake_authorized, sweep_stake_vote_account_address) =
            rpc_client_utils::get_stake_authorized(rpc_client, *sweep_stake_address)?;

        if sweep_stake_authorized.staker != sweep_stake_authority_keypair.pubkey() {
            return Err("Stake authority mismatch".into());
        }

        instructions.append(&mut vec![
            system_instruction::allocate(
                &transitory_stake_account.pubkey(),
                std::mem::size_of::<solana_sdk::stake::state::StakeStateV2>() as u64,
            ),
            system_instruction::assign(
                &transitory_stake_account.pubkey(),
                &solana_sdk::stake::program::id(),
            ),
            solana_sdk::stake::instruction::initialize(
                &transitory_stake_account.pubkey(),
                &sweep_stake_authorized,
                &solana_sdk::stake::state::Lockup::default(),
            ),
            solana_sdk::stake::instruction::delegate_stake(
                &transitory_stake_account.pubkey(),
                &sweep_stake_authority_keypair.pubkey(),
                &sweep_stake_vote_account_address,
            ),
        ]);
        format!(
            "Sweeping {}{} from {} into {} (via {})",
            token.symbol(),
            token
                .ui_amount(sweep_amount)
                .separated_string_with_fixed_place(2),
            from_address,
            sweep_stake_address,
            to_address
        )
    } else {
        format!(
            "Sweeping {}{} from {} into {}",
            token.symbol(),
            token
                .ui_amount(sweep_amount)
                .separated_string_with_fixed_place(2),
            from_address,
            to_address
        )
    };

    let (signature, maybe_transaction) = match existing_signature {
        None => {
            apply_priority_fee(rpc_clients, &mut instructions, 7_000, priority_fee)?;

            let mut message = Message::new(&instructions, Some(&from_authority_address));
            message.recent_blockhash = recent_blockhash;
            let fee = rpc_client.get_fee_for_message(&message)?;
            if fee > authority_account.lamports {
                return Err(format!(
                    "Authority has insufficient funds for the transaction fee of {}",
                    token.ui_amount(fee)
                )
                .into());
            }

            let mut transaction = Transaction::new_unsigned(message);
            let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
            if simulation_result.err.is_some() {
                return Err(format!("Simulation failure: {simulation_result:?}").into());
            }

            transaction.partial_sign(&signers, recent_blockhash);
            if let Some((transitory_stake_account, sweep_stake_authority_keypair, ..)) =
                via_transitory_stake.as_ref()
            {
                assert!(existing_signature.is_none());
                transaction.try_sign(
                    &[transitory_stake_account, sweep_stake_authority_keypair],
                    recent_blockhash,
                )?;
            }

            let signature = transaction.signatures[0];
            println!("Transaction signature: {signature}");

            let epoch = rpc_client.get_epoch_info()?.epoch;
            if let Some((transitory_stake_account, ..)) = via_transitory_stake.as_ref() {
                assert!(existing_signature.is_none());
                db.add_transitory_sweep_stake_address(transitory_stake_account.pubkey(), epoch)?;
            }
            (signature, Some(transaction))
        }
        Some(existing_signature) => (existing_signature, None),
    };
    db.record_transfer(
        signature,
        last_valid_block_height,
        Some(sweep_amount),
        from_address,
        token,
        to_address,
        token,
        LotSelectionMethod::default(),
        None,
    )?;

    if let Some(transaction) = maybe_transaction {
        if !send_transaction_until_expired(rpc_clients, &transaction, last_valid_block_height)
            .unwrap_or_default()
        {
            db.cancel_transfer(signature)?;
            if let Some((transitory_stake_account, ..)) = via_transitory_stake.as_ref() {
                db.remove_transitory_sweep_stake_address(transitory_stake_account.pubkey())?;
            }
            return Err("Sweep failed".into());
        }
    }
    println!("Confirming sweep: {signature}");
    let when = get_signature_date(rpc_client, signature).await?;
    db.confirm_transfer(signature, when)?;

    notifier.send(&msg).await;
    println!("{msg}");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_account_split<T: Signers>(
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
            println!(
                "Split declined because {:?} balance is less than {}",
                from_address,
                token.format_ui_amount(if_balance_exceeds)
            );
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

    println!(
        "Splitting {} from {} into {}",
        token.ui_amount(amount),
        from_address,
        into_keypair.pubkey(),
    );

    transaction.try_partial_sign(&signers, recent_blockhash)?;
    transaction.try_sign(&[&into_keypair], recent_blockhash)?;

    let signature = transaction.signatures[0];
    println!("Transaction signature: {signature}");

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
    println!("Split confirmed: {signature}");
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
async fn process_account_redelegate<T: Signers>(
    db: &mut Db,
    rpc_clients: &RpcClients,
    from_address: Pubkey,
    vote_account_address: Pubkey,
    lot_selection_method: LotSelectionMethod,
    authority_address: Pubkey,
    signers: &T,
    into_keypair: Option<Keypair>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_client = rpc_clients.default();
    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

    let minimum_stake_account_balance = rpc_client
        .get_minimum_balance_for_rent_exemption(solana_sdk::stake::state::StakeStateV2::size_of())?;

    let into_keypair = into_keypair.unwrap_or_else(Keypair::new);
    if db
        .get_account(into_keypair.pubkey(), MaybeToken::SOL())
        .is_some()
    {
        return Err(format!(
            "Account {} ({}) already exists",
            into_keypair.pubkey(),
            MaybeToken::SOL()
        )
        .into());
    }

    let from_account = db
        .get_account(from_address, MaybeToken::SOL())
        .ok_or_else(|| format!("SOL account does not exist for {from_address}"))?;

    if from_account.last_update_balance < minimum_stake_account_balance * 2 {
        return Err(format!(
            "Account {} ({}) has insufficient balance",
            into_keypair.pubkey(),
            MaybeToken::SOL()
        )
        .into());
    }
    let redelegated_amount = from_account.last_update_balance - minimum_stake_account_balance;

    let instructions = solana_sdk::stake::instruction::redelegate(
        &from_address,
        &authority_address,
        &vote_account_address,
        &into_keypair.pubkey(),
    );

    let message = Message::new(&instructions, Some(&authority_address));

    let mut transaction = Transaction::new_unsigned(message);
    transaction.message.recent_blockhash = recent_blockhash;
    let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
    if simulation_result.err.is_some() {
        return Err(format!("Simulation failure: {simulation_result:?}").into());
    }

    println!(
        "Relegating {} to {} via{}",
        from_address,
        vote_account_address,
        into_keypair.pubkey(),
    );

    transaction.partial_sign(signers, recent_blockhash);
    transaction.try_sign(&[&into_keypair], recent_blockhash)?;

    let signature = transaction.signatures[0];
    println!("Transaction signature: {signature}");

    let epoch = rpc_client.get_epoch_info()?.epoch;
    db.add_account(TrackedAccount {
        address: into_keypair.pubkey(),
        token: MaybeToken::SOL(),
        description: from_account.description,
        last_update_epoch: epoch.saturating_sub(1),
        last_update_balance: 0,
        lots: vec![],
        no_sync: None,
    })?;
    db.record_transfer(
        signature,
        last_valid_block_height,
        Some(redelegated_amount),
        from_address,
        MaybeToken::SOL(),
        into_keypair.pubkey(),
        MaybeToken::SOL(),
        lot_selection_method,
        None,
    )?;

    if !send_transaction_until_expired(rpc_clients, &transaction, last_valid_block_height)
        .unwrap_or_default()
    {
        db.cancel_transfer(signature)?;
        db.remove_account(into_keypair.pubkey(), MaybeToken::SOL())?;
        return Err("Redelegate failed".into());
    }
    println!("Redelegation confirmed: {signature}");
    let when = get_signature_date(rpc_client, signature).await?;
    db.confirm_transfer(signature, when)?;

    Ok(())
}

async fn process_account_sync(
    db: &mut Db,
    rpc_clients: &RpcClients,
    address: Option<Pubkey>,
    max_epochs_to_process: Option<u64>,
    reconcile_no_sync_account_balances: bool,
    force_rescan_balances: bool,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_client = rpc_clients.default();
    process_account_sync_pending_transfers(db, rpc_client).await?;
    process_account_sync_sweep(db, rpc_clients, notifier).await?;

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
                    println!(
                        "\nWarning: {} ({}) balance is less than expected. Actual: {}{}, expected: {}{}\n",
                        account.address,
                        account.token,
                        account.token.symbol(),
                        account.token.ui_amount(current_balance),
                        account.token.symbol(),
                        account.token.ui_amount(account.last_update_balance)
                    );
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
                    println!("{msg}");

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
        println!("Processed up to epoch {stop_epoch}");
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
        println!("{msg}");

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
                println!("{msg}");

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
            println!(
                "\nWarning: {} ({}) balance is less than expected. Actual: {}{}, expected: {}{}\n",
                account.address,
                account.token,
                account.token.symbol(),
                account.token.ui_amount(current_balance),
                account.token.symbol(),
                account.token.ui_amount(account.last_update_balance)
            );
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
            println!("{msg}");

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
            )
            .await;
            account.lots.push(lot);
            account.last_update_balance = current_balance;
        }

        db.update_account(account.clone())?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_account_wrap<T: Signers>(
    db: &mut Db,
    rpc_clients: &RpcClients,
    address: Pubkey,
    amount: Amount,
    if_source_balance_exceeds: Option<u64>,
    lot_selection_method: LotSelectionMethod,
    lot_numbers: Option<HashSet<usize>>,
    authority_address: Pubkey,
    signers: T,
    priority_fee: PriorityFee,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_client = rpc_clients.default();
    let sol = MaybeToken::SOL();
    let wsol = Token::wSOL;
    let wsol_address = wsol.ata(&address);

    let from_account = db
        .get_account(address, sol)
        .ok_or_else(|| format!("SOL account does not exist for {address}"))?;
    let amount = amount.unwrap_or(from_account.last_update_balance);

    if let Some(if_source_balance_exceeds) = if_source_balance_exceeds {
        if from_account.last_update_balance < if_source_balance_exceeds {
            println!(
                "wrap declined because {} balance is less than {}{}",
                address,
                sol.symbol(),
                sol.ui_amount(if_source_balance_exceeds)
            );
            return Ok(());
        }
    }

    if amount == 0 {
        println!("Nothing to wrap");
        return Ok(());
    }

    if db.get_account(address, wsol.into()).is_none() {
        let epoch = rpc_client.get_epoch_info()?.epoch;
        db.add_account(TrackedAccount {
            address,
            token: wsol.into(),
            description: from_account.description,
            last_update_epoch: epoch,
            last_update_balance: 0,
            lots: vec![],
            no_sync: None,
        })?;
    }

    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

    let mut instructions = vec![];
    instructions.extend([
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &authority_address,
            &address,
            &wsol.mint(),
            &wsol.program_id(),
        ),
        system_instruction::transfer(&address, &wsol_address, amount),
        spl_token::instruction::sync_native(&wsol.program_id(), &wsol_address).unwrap(),
    ]);

    apply_priority_fee(rpc_clients, &mut instructions, 30_000, priority_fee)?;
    let message = Message::new(&instructions, Some(&authority_address));

    let mut transaction = Transaction::new_unsigned(message);
    transaction.message.recent_blockhash = recent_blockhash;
    let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
    if simulation_result.err.is_some() {
        return Err(format!("Simulation failure: {simulation_result:?}").into());
    }

    println!("Wrapping {} for {}", wsol.ui_amount(amount), address);

    transaction.try_sign(&signers, recent_blockhash)?;

    let signature = transaction.signatures[0];
    println!("Transaction signature: {signature}");

    db.record_transfer(
        signature,
        last_valid_block_height,
        Some(amount),
        address,
        sol,
        address,
        wsol.into(),
        lot_selection_method,
        lot_numbers,
    )?;

    if !send_transaction_until_expired(rpc_clients, &transaction, last_valid_block_height)
        .unwrap_or_default()
    {
        db.cancel_transfer(signature)?;
        return Err("Wrap failed".into());
    }
    println!("Wrap confirmed: {signature}");
    let when = get_signature_date(rpc_client, signature).await?;
    db.confirm_transfer(signature, when)?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_account_unwrap<T: Signers>(
    db: &mut Db,
    rpc_clients: &RpcClients,
    address: Pubkey,
    amount: Option<u64>,
    lot_selection_method: LotSelectionMethod,
    lot_numbers: Option<HashSet<usize>>,
    authority_address: Pubkey,
    signers: T,
    priority_fee: PriorityFee,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_client = rpc_clients.default();
    let sol = MaybeToken::SOL();
    let wsol = Token::wSOL;

    let from_account = db
        .get_account(address, wsol.into())
        .ok_or_else(|| format!("Wrapped SOL account does not exist for {address}"))?;
    let amount = amount.unwrap_or(from_account.last_update_balance);

    let _to_account = db
        .get_account(address, sol)
        .ok_or_else(|| format!("SOL account does not exist for {address}"))?;

    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

    let ephemeral_token_account = Keypair::new();

    let mut instructions = vec![
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &authority_address,
            &ephemeral_token_account.pubkey(),
            &wsol.mint(),
            &wsol.program_id(),
        ),
        spl_token::instruction::transfer_checked(
            &wsol.program_id(),
            &wsol.ata(&address),
            &wsol.mint(),
            &wsol.ata(&ephemeral_token_account.pubkey()),
            &authority_address,
            &[],
            amount,
            wsol.decimals(),
        )
        .unwrap(),
        spl_token::instruction::close_account(
            &wsol.program_id(),
            &wsol.ata(&ephemeral_token_account.pubkey()),
            &address,
            &ephemeral_token_account.pubkey(),
            &[],
        )
        .unwrap(),
    ];
    apply_priority_fee(rpc_clients, &mut instructions, 30_000, priority_fee)?;

    let message = Message::new(&instructions, Some(&authority_address));

    let mut transaction = Transaction::new_unsigned(message);
    transaction.message.recent_blockhash = recent_blockhash;
    let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
    if simulation_result.err.is_some() {
        return Err(format!("Simulation failure: {simulation_result:?}").into());
    }

    println!("Unwrapping {} for {}", wsol.ui_amount(amount), address);

    transaction.partial_sign(&signers, recent_blockhash);
    transaction.try_sign(&[&ephemeral_token_account], recent_blockhash)?;

    let signature = transaction.signatures[0];
    println!("Transaction signature: {signature}");

    db.record_transfer(
        signature,
        last_valid_block_height,
        Some(amount),
        address,
        wsol.into(),
        address,
        sol,
        lot_selection_method,
        lot_numbers,
    )?;

    if !send_transaction_until_expired(rpc_clients, &transaction, last_valid_block_height)
        .unwrap_or_default()
    {
        db.cancel_transfer(signature)?;
        return Err("Wrap failed".into());
    }
    println!("Unwrap confirmed: {signature}");
    let when = get_signature_date(rpc_client, signature).await?;
    db.confirm_transfer(signature, when)?;

    Ok(())
}

async fn process_account_sync_pending_transfers(
    db: &mut Db,
    rpc_client: &RpcClient,
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
                    println!("Pending transfer confirmed: {signature}");
                    let when = get_signature_date(rpc_client, signature).await?;
                    db.confirm_transfer(signature, when)?;
                } else {
                    println!("Pending transfer failed with {result:?}: {signature}");
                    db.cancel_transfer(signature)?;
                }
            }
            None => {
                if block_height > last_valid_block_height {
                    println!("Pending transfer cancelled: {signature}");
                    db.cancel_transfer(signature)?;
                } else {
                    println!(
                        "Transfer pending for at most {} blocks: {}",
                        last_valid_block_height.saturating_sub(block_height),
                        signature
                    );
                }
            }
        }
    }
    Ok(())
}

async fn process_account_sync_sweep(
    db: &mut Db,
    rpc_clients: &RpcClients,
    _notifier: &Notifier,
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

    #[allow(deprecated)]
    let sweep_stake_activation = rpc_client
        .get_stake_activation(sweep_stake_account_info.address, None)
        .map_err(|err| {
            format!(
                "Unable to get activation information for sweep stake account: {}: {}",
                sweep_stake_account_info.address, err
            )
        })?;

    if sweep_stake_activation.state != StakeActivationState::Active {
        println!(
            "Sweep stake account is not active, unable to continue: {sweep_stake_activation:?}"
        );
        return Ok(());
    }

    for transitory_sweep_stake_address in transitory_sweep_stake_addresses {
        println!("Considering merging transitory stake {transitory_sweep_stake_address}");

        let transitory_sweep_stake_account = match rpc_client
            .get_account_with_commitment(&transitory_sweep_stake_address, rpc_client.commitment())?
            .value
        {
            None => {
                println!(
                    "  Transitory sweep stake account does not exist, removing it: {transitory_sweep_stake_address}"
                );

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

        #[allow(deprecated)]
        let transient_stake_activation = rpc_client
            .get_stake_activation(transitory_sweep_stake_address, None)
            .map_err(|err| {
                format!(
                    "Unable to get activation information for transient stake: {transitory_sweep_stake_address}: {err}"
                )
            })?;

        if transient_stake_activation.state != StakeActivationState::Active {
            println!("  Transitory stake is not yet active: {transient_stake_activation:?}");
            continue;
        }

        if !rpc_client_utils::stake_accounts_have_same_credits_observed(
            &sweep_stake_account,
            &transitory_sweep_stake_account,
        )? {
            println!(
                "  Transitory stake credits observed mismatch with sweep stake account: {transitory_sweep_stake_address}"
            );
            continue;
        }
        println!("  Merging into sweep stake account");

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
        println!("Transaction signature: {signature}");
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

fn lot_numbers_of(matches: &ArgMatches<'_>, name: &str) -> Option<HashSet<usize>> {
    values_t!(matches, name, usize)
        .ok()
        .map(|x| x.into_iter().collect())
}

fn lot_numbers_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("lot_numbers")
        .long("lot")
        .value_name("LOT NUMBER")
        .takes_value(true)
        .multiple(true)
        .validator(is_parsable::<usize>)
        .help("Lot to fund the wrap from")
}

fn lot_selection_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("lot_selection")
        .long("lot-selection")
        .value_name("METHOD")
        .takes_value(true)
        .validator(is_parsable::<LotSelectionMethod>)
        .default_value(POSSIBLE_LOT_SELECTION_METHOD_VALUES[0])
        .possible_values(POSSIBLE_LOT_SELECTION_METHOD_VALUES)
        .help("Lot selection method")
}

fn is_tax_rate(s: String) -> Result<(), String> {
    is_parsable::<f64>(s.clone())?;
    let f = s.parse::<f64>().unwrap();
    if (0. ..=1.).contains(&f) {
        Ok(())
    } else {
        Err(format!("rate must be in the range [0,1]: {f}"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    solana_logger::setup_with_default("solana=info");
    let default_db_path = "sell-your-sol";
    let default_json_rpc_url = "https://api.mainnet-beta.solana.com";
    let default_when = {
        let today = Local::now().date_naive();
        format!("{}/{}/{}", today.year(), today.month(), today.day())
    };
    let exchanges = ["binance", "binanceus", "coinbase", "kraken"];

    let app_version = &*app_version();
    let mut app = App::new(crate_name!())
        .about(crate_description!())
        .version(app_version)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .setting(AppSettings::InferSubcommands)
        .arg(
            Arg::with_name("db_path")
                .long("db-path")
                .value_name("PATH")
                .takes_value(true)
                .default_value(default_db_path)
                .global(true)
                .help("Database path"),
        )
        .arg(
            Arg::with_name("json_rpc_url")
                .short("u")
                .long("url")
                .value_name("URL")
                .takes_value(true)
                .global(true)
                .validator(is_url_or_moniker)
                .default_value(default_json_rpc_url)
                .help("JSON RPC URL for the cluster"),
        )
        .arg(
            Arg::with_name("send_json_rpc_urls")
                .long("send-url")
                .value_name("URL")
                .takes_value(true)
                .validator(is_comma_separated_url_or_moniker_list)
                .help("Optional additional JSON RPC URLs, separated by commas, to \
                       submit transactions with in addition to --url"),
        )
        .arg(
            Arg::with_name("helius_json_rpc_url")
                .long("helius-url")
                .value_name("URL")
                .takes_value(true)
                .global(true)
                .validator(is_url)
                .help("Helius JSON RPC URL to use only for the proprietary getPriorityFeeEstimate RPC method"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .takes_value(false)
                .global(true)
                .help("Show additional information"),
        )
        .arg(
            Arg::with_name("priority_fee_exact")
                .long("priority-fee-exact")
                .value_name("SOL")
                .takes_value(true)
                .validator(is_parsable::<f64>)
                .help("Exactly specify the Solana priority fee to use for transactions"),
        )
        .arg(
            Arg::with_name("priority_fee_auto")
                .long("priority-fee-auto")
                .value_name("SOL")
                .takes_value(true)
                .conflicts_with("priority_fee_exact")
                .validator(is_parsable::<f64>)
                .help("Automatically select the Solana priority fee to use for transactions, \
                       but do not exceed the specified amount of SOL [default]"),
        )
        .subcommand(
            SubCommand::with_name("price")
                .about("Get token price")
                .arg(
                    Arg::with_name("token")
                        .value_name("SOL or SPL Token")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_token_or_sol)
                        .default_value("SOL")
                        .help("Token type"),
                )
                .arg(
                    Arg::with_name("when")
                        .value_name("YY/MM/DD")
                        .takes_value(true)
                        .required(false)
                        .validator(|value| naivedate_of(&value).map(|_| ()))
                        .help("Date to fetch the price for [default: current spot price]"),
                )
        )
        .subcommand(
            SubCommand::with_name("sync")
                .about("Synchronize with all exchanges and accounts"))
                .arg(
                    Arg::with_name("max_epochs_to_process")
                        .long("max-epochs-to-process")
                        .value_name("NUMBER")
                        .takes_value(true)
                        .validator(is_parsable::<u64>)
                        .help("Only process up to this number of epochs for account balance changes [default: all]"),
                )
        .subcommand(
            SubCommand::with_name("db")
                .about("Database management")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .setting(AppSettings::InferSubcommands)
                .subcommand(
                    SubCommand::with_name("import")
                        .about("Import another database")
                        .arg(
                            Arg::with_name("other_db_path")
                                .value_name("PATH")
                                .takes_value(true)
                                .help("Path to the database to import"),
                        )
                )
        )
        .subcommand(
            SubCommand::with_name("influxdb")
                .about("InfluxDb metrics management")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .setting(AppSettings::InferSubcommands)
                .subcommand(
                    SubCommand::with_name("clear")
                        .about("Clear InfluxDb configuration")
                )
                .subcommand(
                    SubCommand::with_name("show")
                        .about("Show InfluxDb configuration")
                )
                .subcommand(
                    SubCommand::with_name("set")
                        .about("Set InfluxDb configuration")
                        .arg(
                            Arg::with_name("url")
                                .value_name("URL")
                                .takes_value(true)
                                .required(true)
                                .help("InfluxDb URL"),
                        )
                        .arg(
                            Arg::with_name("token")
                                .value_name("TOKEN")
                                .takes_value(true)
                                .required(true)
                                .help("Access Token"),
                        )
                        .arg(
                            Arg::with_name("org")
                                .value_name("ORG")
                                .takes_value(true)
                                .required(true)
                                .help("Organization"),
                        )
                        .arg(
                            Arg::with_name("bucket")
                                .value_name("BUCKET")
                                .takes_value(true)
                                .required(true)
                                .help("Bucket name"),
                        )
                )
        )
        .subcommand(
            SubCommand::with_name("account")
                .about("Account management")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .setting(AppSettings::InferSubcommands)
                .subcommand(
                    SubCommand::with_name("add")
                        .about("Register an account")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .help("Token type"),
                        )
                        .arg(
                            Arg::with_name("address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Account address to add"),
                        )
                        .arg(
                            Arg::with_name("description")
                                .short("d")
                                .long("description")
                                .value_name("TEXT")
                                .takes_value(true)
                                .help("Account description"),
                        )
                        .arg(
                            Arg::with_name("when")
                                .short("w")
                                .long("when")
                                .value_name("YY/MM/DD")
                                .takes_value(true)
                                .validator(|value| naivedate_of(&value).map(|_| ()))
                                .help("Date acquired (ignored if the --transaction argument is provided) [default: now]"),
                        )
                        .arg(
                            Arg::with_name("transaction")
                                .short("t")
                                .long("transaction")
                                .value_name("SIGNATURE")
                                .takes_value(true)
                                .validator(is_parsable::<Signature>)
                                .help("Acquisition transaction signature"),
                        )
                        .arg(
                            Arg::with_name("price")
                                .short("p")
                                .long("price")
                                .value_name("USD")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .help("Acquisition price per SOL/token [default: market price on acquisition date]"),
                        )
                        .arg(
                            Arg::with_name("income")
                                .long("income")
                                .takes_value(false)
                                .conflicts_with("transaction")
                                .help("Consider the acquisition value to be subject to income tax [default: post-tax fiat]"),
                        )
                        .arg(
                            Arg::with_name("no_sync")
                                .long("no-sync")
                                .takes_value(false)
                                .help("Never synchronize this account with the on-chain state (advanced; uncommon)"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .long("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .conflicts_with("transaction")
                                .help("Consider the account to have this amount of tokens rather than \
                                       using the current value on chain (advanced; uncommon)"),
                        )
                        .arg(
                            Arg::with_name("neg_amount")
                                .long("neg-amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .conflicts_with("amount")
                                .conflicts_with("transaction")
                                .help("If a negative amount is specified, subtract the provided AMOUNT from the \
                                       on-chain balance (advanced; uncommon)"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("dispose")
                        .about("Manually record the disposal of SOL/tokens from an account")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .help("Token type"),
                        )
                        .arg(
                            Arg::with_name("address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Account that the SOL/tokens was/where disposed from"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .required(true)
                                .help("Amount of SOL/tokens that was/where disposed from the account"),
                        )
                        .arg(
                            Arg::with_name("description")
                                .short("d")
                                .long("description")
                                .value_name("TEXT")
                                .takes_value(true)
                                .help("Description to associate with the disposal event"),
                        )
                        .arg(
                            Arg::with_name("when")
                                .short("w")
                                .long("when")
                                .value_name("YY/MM/DD")
                                .takes_value(true)
                                .validator(|value| naivedate_of(&value).map(|_| ()))
                                .help("Disposal date [default: now]"),
                        )
                        .arg(
                            Arg::with_name("price")
                                .short("p")
                                .long("price")
                                .value_name("USD")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .help("Disposal price per SOL/token [default: market price on disposal date]"),
                        )
                        .arg(lot_selection_arg())
                        .arg(lot_numbers_arg()),
                )
                .subcommand(
                    SubCommand::with_name("drop")
                        .about("Manually drop SOL/tokens from an account")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .help("Token type"),
                        )
                        .arg(
                            Arg::with_name("address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Account that the SOL/tokens should be dropped from"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .required(true)
                                .help("Amount of SOL/tokens to drop"),
                        )
                        .arg(
                            Arg::with_name("confirm")
                                .long("confirm")
                                .takes_value(false)
                                .help("Confirm the operation"),
                        )
                        .arg(lot_selection_arg())
                        .arg(lot_numbers_arg()),
                )
                .subcommand(
                    SubCommand::with_name("ls")
                        .about("List registered accounts")
                        .alias("sl")
                        .arg(
                            Arg::with_name("all")
                                .short("a")
                                .long("all")
                                .help("Display all lots")
                        )
                        .arg(
                            Arg::with_name("account")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .validator(is_valid_pubkey)
                                .help("Limit output to this address"),
                        )
                        .arg(
                            Arg::with_name("summary")
                                .long("summary")
                                .takes_value(false)
                                .help("Limit output to summary line"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("cost-basis")
                        .about("Display average cost basis of holdings")
                        .arg(
                            Arg::with_name("when")
                                .value_name("YY/MM/DD")
                                .takes_value(true)
                                .required(false)
                                .validator(|value| naivedate_of(&value).map(|_| ()))
                                .default_value(&default_when)
                                .help("Date to calculate cost basis for")
                        )
                )
                .subcommand(
                    SubCommand::with_name("xls")
                        .about("Export an Excel spreadsheet file")
                        .arg(
                            Arg::with_name("outfile")
                                .value_name("FILEPATH")
                                .takes_value(true)
                                .help(".xls file to write"),
                        )
                        .arg(
                            Arg::with_name("year")
                                .long("year")
                                .value_name("YYYY")
                                .takes_value(true)
                                .validator(is_parsable::<usize>)
                                .help("Limit export to realized gains affecting the given year"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("csv")
                        .about("Export cap gains to a CSV file importable into TurboTax")
                        .arg(
                            Arg::with_name("outfile")
                                .value_name("FILEPATH")
                                .takes_value(true)
                                .help(".csv file to write"),
                        )
                        .arg(
                            Arg::with_name("year")
                                .long("year")
                                .value_name("YYYY")
                                .takes_value(true)
                                .validator(is_parsable::<usize>)
                                .help("Limit export to realized gains affecting the given year"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("remove")
                        .about("Unregister an account")
                        .alias("delete")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .help("Token type"),
                        )
                        .arg(
                            Arg::with_name("address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Account address to remove"),
                        )
                        .arg(
                            Arg::with_name("confirm")
                                .long("confirm")
                                .takes_value(false)
                                .help("Confirm the operation"),
                        )
                        .arg(
                            Arg::with_name("proceed_even_if_lots_exist")
                                .long("proceed-even-if-lots-exist")
                                .takes_value(false)
                                .help("Proceed even if the account has lots (advanced; uncommon)"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("set-sweep-stake-account")
                        .about("Set the sweep stake account")
                        .arg(
                            Arg::with_name("address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Sweep stake account address"),
                        )
                        .arg(
                            Arg::with_name("stake_authority")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .required(true)
                                .help("Stake authority keypair"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("set-tax-rate")
                        .about("Set entity tax rate for account listing")
                        .arg(
                            Arg::with_name("income")
                                .takes_value(true)
                                .required(true)
                                .validator(is_tax_rate)
                                .help("Income tax rate")
                        )
                        .arg(
                            Arg::with_name("short-term-gain")
                                .takes_value(true)
                                .required(true)
                                .validator(is_tax_rate)
                                .help("Short-term capital gain tax rate")
                        )
                        .arg(
                            Arg::with_name("long-term-gain")
                                .takes_value(true)
                                .validator(is_tax_rate)
                                .help("Long-term capital gain tax rate (default: short-term rate)")
                        )
                )
                .subcommand(
                    SubCommand::with_name("tax-rate")
                        .about("Show entity tax rate for account listing")
                )
                .subcommand(
                    SubCommand::with_name("merge")
                        .about("Merge one stake account into another")
                        .arg(
                            Arg::with_name("from_address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Source address")
                        )
                        .arg(
                            Arg::with_name("into_address")
                                .long("into")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Destination address")
                        )
                        .arg(
                            Arg::with_name("by")
                                .long("by")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Optional authority for the merge"),
                        )
                        .arg(
                            Arg::with_name("transaction")
                                .long("transaction")
                                .value_name("SIGNATURE")
                                .takes_value(true)
                                .validator(is_parsable::<Signature>)
                                .help("Use an existing transaction signature for merge. \
                                      That is, perform the local database operations only. \
                                      Careful!")
                        )
                )
                .subcommand(
                    SubCommand::with_name("sweep")
                        .about("Sweep SOL into the sweep stake account")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .default_value("SOL")
                                .help("Token type"),
                        )
                        .arg(
                            Arg::with_name("address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Source address to sweep from"),
                        )
                        .arg(
                            Arg::with_name("authority")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_signer)
                                .help("Source account authority keypair"),
                        )
                        .arg(
                            Arg::with_name("to")
                                .long("to")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .validator(is_valid_pubkey)
                                .help("Sweep destination address [default: sweep stake account]")
                        )
                        .arg(
                            Arg::with_name("no_sweep_ok")
                                .long("no-sweep-ok")
                                .takes_value(false)
                                .help("Exit successfully if a sweep is not possible due to low source account balance"),
                        )
                        .arg(
                            Arg::with_name("exactly")
                                .long("exactly")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .help("Sweep exactly this amount [default: full account balance minus the value provided to --retain]"),
                        )
                        .arg(
                            Arg::with_name("retain")
                                .short("r")
                                .long("retain")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .help("Amount of SOL/tokens to leave in source account [default: 0]"),
                        )
                        .arg(
                            Arg::with_name("transaction")
                                .long("transaction")
                                .value_name("SIGNATURE")
                                .takes_value(true)
                                .validator(is_parsable::<Signature>)
                                .help("Use an existing transaction signature for sweep. \
                                      That is, perform the local database operations only. \
                                      Careful!")
                        )
                )
                .subcommand(
                    SubCommand::with_name("split")
                        .about("Split a stake account")
                        .arg(
                            Arg::with_name("from_address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Address of the stake account to split")
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount_or_all)
                                .required(true)
                                .help("The amount to wrap, in SOL; accepts keyword ALL"),
                        )
                        .arg(
                            Arg::with_name("description")
                                .short("d")
                                .long("description")
                                .value_name("TEXT")
                                .takes_value(true)
                                .help("Description of the new account"),
                        )
                        .arg(
                            Arg::with_name("by")
                                .long("by")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Optional authority for the split"),
                        )
                        .arg(
                            Arg::with_name("into_keypair")
                                .long("into")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .validator(is_keypair)
                                .help("Optional keypair of the split destination [default: randomly generated]"),
                        )
                        .arg(
                            Arg::with_name("if_balance_exceeds")
                                .long("if-balance-exceeds")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .help(
                                    "Exit successfully without performing the split if \
                                       the account balance is less than this amount",
                                ),
                        )
                        .arg(lot_selection_arg())
                        .arg(lot_numbers_arg())
                )
                .subcommand(
                    SubCommand::with_name("redelegate")
                        .about("Redelegate a stake account to another validator")
                        .arg(
                            Arg::with_name("from_address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Address of the stake account to redelegate")
                        )
                        .arg(
                            Arg::with_name("vote_account_address")
                                .long("to")
                                .value_name("VOTE ACCOUNT")
                                .takes_value(true)
                                .validator(is_valid_pubkey)
                                .required(true)
                                .help("Address of the redelegated validator vote account"),
                        )
                        .arg(
                            Arg::with_name("by")
                                .long("by")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Optional authority for the redelegation"),
                        )
                        .arg(
                            Arg::with_name("into_keypair")
                                .long("into")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .validator(is_keypair)
                                .help("Optional keypair for the redelegated stake account [default: randomly generated]"),
                        )
                        .arg(lot_selection_arg())
                )
                .subcommand(
                    SubCommand::with_name("sync")
                        .about("Synchronize an account address")
                        .arg(
                            Arg::with_name("address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(false)
                                .validator(is_valid_pubkey)
                                .help("Account to synchronize"),
                        )
                        .arg(
                            Arg::with_name("max_epochs_to_process")
                                .long("max-epochs-to-process")
                                .value_name("NUMBER")
                                .takes_value(true)
                                .validator(is_parsable::<u64>)
                                .help("Only process up to this number of epochs for account balance changes [default: all]"),
                        )
                        .arg(
                            Arg::with_name("reconcile_no_sync_account_balances")
                                .long("reconcile-no-sync-account-balances")
                                .takes_value(false)
                                .help("Reconcile local account balances with on-chain state for --no-sync accounts (advanced; uncommon)"),
                        )
                        .arg(
                            Arg::with_name("force_rescan_balances")
                                .long("force-rescan-balances")
                                .takes_value(false)
                                .help("Rescan for account balance changes even in same epoch (advanced; uncommon)"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("wrap")
                        .about("Wrap SOL into wSOL")
                        .arg(
                            Arg::with_name("address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Address of the account to wrap")
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount_or_all_or_half)
                                .required(true)
                                .help("The amount to wrap, in SOL; accepts keywords ALL and HALF"),
                        )
                        .arg(
                            Arg::with_name("by")
                                .long("by")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Optional authority for the wrap"),
                        )
                        .arg(
                            Arg::with_name("if_source_balance_exceeds")
                                .long("if-source-balance-exceeds")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .help(
                                    "Exit successfully without wrapping if the \
                                       source account balance is less than this amount",
                                ),
                        )
                        .arg(lot_selection_arg())
                        .arg(lot_numbers_arg())
                )
                .subcommand(
                    SubCommand::with_name("unwrap")
                        .about("Unwrap SOL from wSOL")
                        .arg(
                            Arg::with_name("address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Address of the account to unwrap")
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount_or_all)
                                .required(true)
                                .help("The amount to unwrap, in SOL; accepts keyword ALL"),
                        )
                        .arg(
                            Arg::with_name("by")
                                .long("by")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Optional authority for the unwrap"),
                        )
                        .arg(lot_selection_arg())
                        .arg(lot_numbers_arg())
                )
                .subcommand(
                    SubCommand::with_name("lot")
                        .about("Account lot management")
                        .setting(AppSettings::SubcommandRequiredElseHelp)
                        .setting(AppSettings::InferSubcommands)
                        .subcommand(
                            SubCommand::with_name("swap")
                                .about("Swap lots")
                                .arg(
                                    Arg::with_name("lot_number1")
                                        .value_name("LOT NUMBER")
                                        .takes_value(true)
                                        .required(true)
                                        .validator(is_parsable::<usize>)
                                        .help("First lot number"),
                                )
                                .arg(
                                    Arg::with_name("lot_number2")
                                        .value_name("LOT NUMBER")
                                        .takes_value(true)
                                        .required(true)
                                        .validator(is_parsable::<usize>)
                                        .help("Second lot number"),
                                )
                        )
                        .subcommand(
                            SubCommand::with_name("collect")
                                .about("Collect non-disposed lots of a desired type into an address")
                                .arg(
                                    Arg::with_name("token")
                                        .value_name("SOL or SPL Token")
                                        .takes_value(true)
                                        .required(true)
                                        .validator(is_valid_token_or_sol)
                                        .help("Token type"),
                                )
                                .arg(
                                    Arg::with_name("address")
                                        .value_name("ADDRESS")
                                        .takes_value(true)
                                        .required(true)
                                        .validator(is_valid_pubkey)
                                        .help("Account address"),
                                )
                                .arg(lot_selection_arg())
                        )
                        .subcommand(
                            SubCommand::with_name("delete")
                                .about("Delete a lot from the local database only. \
                                        Useful if the on-chain state is out of sync with the database")
                                .arg(
                                    Arg::with_name("lot_numbers")
                                        .value_name("LOT NUMBER")
                                        .takes_value(true)
                                        .required(true)
                                        .multiple(true)
                                        .validator(is_parsable::<usize>)
                                        .help("Lot numbers to delete. Must not be a disposed lot"),
                                )
                                .arg(
                                    Arg::with_name("confirm")
                                        .long("confirm")
                                        .takes_value(false)
                                        .help("Confirm the operation"),
                                )
                        )
                        .subcommand(
                            SubCommand::with_name("move")
                                .about("Move a lot to a new address. \
                                        Useful if the on-chain state is out of sync with the database")
                                .arg(
                                    Arg::with_name("lot_number")
                                        .value_name("LOT NUMBER")
                                        .takes_value(true)
                                        .required(true)
                                        .validator(is_parsable::<usize>)
                                        .help("Lot number to move. Must not be a disposed lot"),
                                )
                                .arg(
                                    Arg::with_name("to_address")
                                        .value_name("RECIPIENT_ADDRESS")
                                        .takes_value(true)
                                        .required(true)
                                        .validator(is_valid_pubkey)
                                        .help("Address to receive the lot"),
                                )
                        ),
                ),
        )
        .subcommand(
            SubCommand::with_name("jup")
                .about("jup.ag")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .setting(AppSettings::InferSubcommands)
                .subcommand(
                    SubCommand::with_name("quote")
                        .about("Get swap quotes")
                        .arg(
                            Arg::with_name("from_token")
                                .value_name("SOURCE TOKEN")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .default_value("SOL")
                                .help("Source token"),
                        )
                        .arg(
                            Arg::with_name("to_token")
                                .value_name("DESTINATION TOKEN")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .default_value("USDC")
                                .help("Destination token"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .required(true)
                                .default_value("1")
                                .help("Amount of the source token to swap"),
                        )
                        .arg(
                            Arg::with_name("slippage_bps")
                                .long("slippage")
                                .value_name("BPS")
                                .takes_value(true)
                                .validator(is_parsable::<u64>)
                                .default_value("100")
                                .help("Maximum slippage bps"),
                        )
                        .arg(
                            Arg::with_name("max_quotes")
                                .short("n")
                                .value_name("LIMIT")
                                .takes_value(true)
                                .validator(is_parsable::<usize>)
                                .help("Limit to this number of quotes [default: all quotes]"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("swap")
                        .about("Swap tokens")
                        .arg(
                            Arg::with_name("address")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_signer)
                                .help("Address of the account holding the tokens to swap")
                        )
                        .arg(
                            Arg::with_name("from_token")
                                .value_name("SOURCE TOKEN")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .help("Source token"),
                        )
                        .arg(
                            Arg::with_name("to_token")
                                .value_name("DESTINATION TOKEN")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .help("Destination token"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("SOURCE TOKEN AMOUNT")
                                .takes_value(true)
                                .validator(is_amount_or_all)
                                .required(true)
                                .help("Amount of tokens to swap; accepts ALL keyword"),
                        )
                        .arg(
                            Arg::with_name("slippage_bps")
                                .long("slippage")
                                .value_name("BPS")
                                .takes_value(true)
                                .validator(is_parsable::<u64>)
                                .default_value("100")
                                .help("Maximum slippage bps"),
                        )
                        .arg(
                            Arg::with_name("if_from_balance_exceeds")
                                .long("if-source-balance-exceeds")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .help(
                                    "Exit successfully without placing a swap if the \
                                       source account balance is less than this amount",
                                ),
                        )
                        .arg(
                            Arg::with_name("for_no_less_than")
                                .long("for-no-less-than")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .conflicts_with("at")
                                .help(
                                    "Exit successfully without swapping if \
                                       the swap would result in less than \
                                       this amount of destination tokens",
                                ),
                        )
                        .arg(
                            Arg::with_name("max_coingecko_value_percentage_loss")
                                .long("max-coingecko-value-percentage-loss")
                                .value_name("PERCENT")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .default_value("5")
                                .help("Reject if the value lost relative to CoinGecko token \
                                      price exceeds this percentage"),
                        )
                        .arg(lot_selection_arg())
                        .arg(
                            Arg::with_name("transaction")
                                .long("transaction")
                                .value_name("SIGNATURE")
                                .takes_value(true)
                                .validator(is_parsable::<Signature>)
                                .help("Existing swap transaction signature that succeeded but \
                                      due to RPC infrastructure limitations the local database \
                                      considered it to have failed. Careful!")
                        )
                )
        );

    for exchange in &exchanges {
        app = app.subcommand(
            SubCommand::with_name(exchange)
                .about("Exchange interactions")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .setting(AppSettings::InferSubcommands)
                .arg(
                    Arg::with_name("exchange_account")
                        .long("account")
                        .value_name("ACCOUNT_NAME")
                        .takes_value(true)
                        .global(true)
                        .help("User-defined name of the exchange account to use \
                              (extra help: specify this option BEFORE the subcomand name, \
                               not after as indicated by --help) [default: '']"),
                )
                .subcommand(
                    SubCommand::with_name("balance")
                        .about("Get exchange balance")
                        .arg(
                            Arg::with_name("available_only")
                                .long("available")
                                .takes_value(false)
                                .help("Only display available balance")
                        )
                        .arg(
                            Arg::with_name("total_only")
                                .long("total")
                                .takes_value(false)
                                .conflicts_with("available_only")
                                .help("Only display total balance")
                        )
                        .arg(
                            Arg::with_name("integer")
                                .long("integer")
                                .takes_value(false)
                                .help("Output integer values with no currency symbols")
                        )
                )
                .subcommand(
                    SubCommand::with_name("address")
                        .about("Show deposit address")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .default_value("SOL")
                                .help("Token type"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("market")
                        .about("Display market info for a given trading pair")
                        .arg(
                            Arg::with_name("pair")
                                .value_name("TRADING_PAIR")
                                .takes_value(true)
                                .help("[default: preferred SOL/USD pair for the exchange]")
                        )
                        .arg(
                            Arg::with_name("ask")
                                .long("ask")
                                .takes_value(false)
                                .help("Only display the current asking price")
                        )
                        .arg(
                            Arg::with_name("weighted_24h_average_price")
                                .long("weighted-24h-average-price")
                                .takes_value(false)
                                .conflicts_with("ask")
                                .help("Only display the weighted average price for the previous 24 hours"),
                        )
                        .arg(
                            Arg::with_name("hourly")
                                .long("hourly")
                                .takes_value(false)
                                .conflicts_with_all(&["ask", "weighted_24h_average_price"])
                                .help("Display hourly price information for the previous 24 hours"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("api")
                        .about("API Management")
                        .setting(AppSettings::SubcommandRequiredElseHelp)
                        .setting(AppSettings::InferSubcommands)
                        .subcommand(
                            SubCommand::with_name("set")
                                .about("Set API key")
                                .arg(Arg::with_name("api_key").required(true).takes_value(true))
                                .arg(Arg::with_name("secret").required(true).takes_value(true))
                                .arg(Arg::with_name("subaccount").takes_value(true)),
                        )
                        .subcommand(SubCommand::with_name("show").about("Show API key"))
                        .subcommand(SubCommand::with_name("clear").about("Clear API key")),
                )
                .subcommand(
                    SubCommand::with_name("deposit")
                        .about("Deposit SOL or SPL Tokens")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .help("Token type"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount_or_all_or_half)
                                .required(true)
                                .help("Amount to deposit; accepts keywords ALL and HALF"),
                        )
                        .arg(lot_selection_arg())
                        .arg(lot_numbers_arg())
                        .arg(
                            Arg::with_name("from")
                                .long("from")
                                .value_name("FROM_ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Source account of funds"),
                        )
                        .arg(
                            Arg::with_name("by")
                                .long("by")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Optional authority of the FROM_ADDRESS"),
                        )
                        .arg(
                            Arg::with_name("if_source_balance_exceeds")
                                .long("if-source-balance-exceeds")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .help(
                                    "Exit successfully without depositing if the \
                                       source account balance is less than this amount",
                                ),
                        )
                        .arg(
                            Arg::with_name("if_exchange_balance_less_than")
                                .long("if-exchange-balance-less-than")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .help(
                                    "Exit successfully without depositing if the \
                                        exchange SOL balance is less than this amount",
                                ),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("withdraw")
                        .about("Withdraw SOL or SPL Tokens")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .help("Token type"),
                        )
                        .arg(
                            Arg::with_name("to")
                                .value_name("RECIPIENT_ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Address to receive the withdrawal of funds"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .required(true)
                                .help("The amount to withdraw; accepts keyword ALL"),
                        )
                        .arg(lot_selection_arg())
                        .arg(lot_numbers_arg())
                        .arg(
                            Arg::with_name("code")
                                .long("code")
                                .value_name("CODE")
                                .takes_value(true)
                                .help("2FA withdrawal code"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("cancel")
                        .about("Cancel orders")
                        .arg(
                            Arg::with_name("order_id")
                                .value_name("ORDER ID")
                                .takes_value(true)
                                .multiple(true)
                                .help("The order id to cancel"),
                        )
                        .arg(
                            Arg::with_name("age")
                                .long("age")
                                .value_name("HOURS")
                                .takes_value(true)
                                .validator(is_parsable::<u32>)
                                .conflicts_with("order_id")
                                .help("Cancel orders older than this number of hours"),
                        )
                        .arg(
                            Arg::with_name("side")
                                .long("side")
                                .required(true)
                                .default_value("both")
                                .possible_values(&["both", "buy", "sell"])
                                .help("Restrict to only buy or sell orders")
                        )
                )
                .subcommand(
                    SubCommand::with_name("buy")
                        .about("Place an order to buy SOL")
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount_or_all)
                                .required(true)
                                .help("The amount to buy, in SOL; accepts keyword ALL"),
                        )
                        .arg(
                            Arg::with_name("at")
                                .long("at")
                                .value_name("PRICE")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .help("Place a limit order at this price"),
                        )
                        .arg(
                            Arg::with_name("bid_minus")
                                .long("bid-minus")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .conflicts_with("at")
                                .validator(is_parsable::<f64>)
                                .help("Place a limit order at this amount under the current bid"),
                        )
                        .arg(
                            Arg::with_name("pair")
                                .long("pair")
                                .value_name("TRADING_PAIR")
                                .takes_value(true)
                                .help("Market to place the order in [default: preferred SOL/USD pair for the exchange]"),
                        )
                        .arg(
                            Arg::with_name("if_balance_exceeds")
                                .long("if-balance-exceeds")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .help(
                                    "Exit successfully without placing a buy order if the \
                                       exchange available balance is less than this amount",
                                ),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("sell")
                        .about("Place an order to sell SOL")
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .required(true)
                                .help("The amount to sell, in SOL"),
                        )
                        .arg(
                            Arg::with_name("at")
                                .long("at")
                                .value_name("PRICE")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .help("Place a limit order at this price"),
                        )
                        .arg(
                            Arg::with_name("ask_plus")
                                .long("ask-plus")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .conflicts_with("at")
                                .validator(is_parsable::<f64>)
                                .help("Place a limit order at this amount over the current ask"),
                        )
                        .arg(lot_selection_arg())
                        .arg(lot_numbers_arg())
                        .arg(
                            Arg::with_name("pair")
                                .long("pair")
                                .value_name("TRADING_PAIR")
                                .takes_value(true)
                                .help("Market to place the order in [default: preferred SOL/USD pair for the exchange]"),
                        )
                        .arg(
                            Arg::with_name("if_balance_exceeds")
                                .long("if-balance-exceeds")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .help(
                                    "Exit successfully without placing a sell order if the \
                                       exchange available balance is less than this amount",
                                ),
                        )
                        .arg(
                            Arg::with_name("if_price_over")
                                .long("if-price-over")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .conflicts_with("at")
                                .help(
                                    "Exit successfully without placing a sell order if the \
                                       order would be placed at a price that is less than \
                                       or equal to this amount",
                                ),
                        )
                        .arg(
                            Arg::with_name("if_price_over_basis")
                                .long("if-price-over-basis")
                                .takes_value(false)
                                .help(
                                    "Exit successfully without placing a sell order if the \
                                       order price would be less than the basis",
                                ),
                        )
                        .arg(
                            Arg::with_name("price_floor")
                                .long("price-floor")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .conflicts_with("if_price_over")
                                .help(
                                    "If the computed price is less than this amount then \
                                       use this amount instead",
                                ),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("pending-deposits")
                        .about("Display pending deposits")
                        .arg(
                            Arg::with_name("quiet")
                                .long("quiet")
                                .takes_value(false)
                                .help(
                                    "Disable output and exit with a non-zero status code \
                                        if any deposits are pending"
                                ),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("pending-withdrawals")
                        .about("Display pending withdrawals")
                        .arg(
                            Arg::with_name("quiet")
                                .long("quiet")
                                .takes_value(false)
                                .help(
                                    "Disable output and exit with a non-zero status code \
                                        if any withdrawals are pending"
                                ),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("lend")
                        .about("Make a lending offer")
                        .arg(
                            Arg::with_name("coin")
                                .value_name("COIN")
                                .takes_value(true)
                                .required(true)
                                .help("The coin to lend"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount_or_all)
                                .help("The amount to lend; accepts keyword ALL"),
                        )
                        .arg(
                            Arg::with_name("available")
                                .short("a")
                                .long("available")
                                .requires("amount")
                                .takes_value(false)
                                .help("Invert AMOUNT to mean, the amount to keep available and lend the rest"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("lending-history")
                        .about("Display lending history")
                        .setting(AppSettings::SubcommandRequiredElseHelp)
                        .setting(AppSettings::InferSubcommands)
                        .subcommand(
                            SubCommand::with_name("range")
                                .about("Display lending history for the given date range")
                                .arg(
                                    Arg::with_name("start_date")
                                        .value_name("YY/MM/DD")
                                        .takes_value(true)
                                        .required(true)
                                        .validator(|value| naivedate_of(&value).map(|_| ()))
                                        .help("Start date, inclusive")
                                )
                                .arg(
                                    Arg::with_name("end_date")
                                        .value_name("YY/MM/DD")
                                        .takes_value(true)
                                        .required(true)
                                        .default_value(&default_when)
                                        .validator(|value| naivedate_of(&value).map(|_| ()))
                                        .help("End date, inclusive")
                                )
                        )
                        .subcommand(
                            SubCommand::with_name("previous")
                                .about("Display lending history for previous days")
                                .arg(
                                    Arg::with_name("days")
                                        .value_name("DAYS")
                                        .default_value("1")
                                        .validator(is_parsable::<usize>)
                                        .help("Number of days, including today")
                                )
                        )
                )
                .subcommand(SubCommand::with_name("sync").about("Synchronize exchange")),
        );
    }

    let app_matches = app.get_matches();
    let db_path = value_t_or_exit!(app_matches, "db_path", PathBuf);
    let verbose = app_matches.is_present("verbose");

    let priority_fee = if let Ok(ui_priority_fee) = value_t!(app_matches, "priority_fee_exact", f64)
    {
        PriorityFee::Exact {
            lamports: sol_to_lamports(ui_priority_fee),
        }
    } else if let Ok(ui_priority_fee) = value_t!(app_matches, "priority_fee_auto", f64) {
        PriorityFee::default_auto_percentile(sol_to_lamports(ui_priority_fee))
    } else {
        PriorityFee::default_auto()
    };

    let rpc_clients = RpcClients::new(
        value_t_or_exit!(app_matches, "json_rpc_url", String),
        value_t!(app_matches, "send_json_rpc_urls", String).ok(),
        value_t!(app_matches, "helius_json_rpc_url", String).ok(),
    );

    let rpc_client = rpc_clients.default();

    let mut wallet_manager = None;
    let notifier = Notifier::default();

    if !db_path.exists() {
        fs::create_dir_all(&db_path)?;
    }

    let mut db_fd_lock = fd_lock::RwLock::new(fs::File::open(&db_path).unwrap());
    let _db_write_lock = loop {
        match db_fd_lock.try_write() {
            Ok(lock) => break lock,
            Err(err) => {
                eprintln!(
                    "Unable to lock database directory: {}: {}",
                    db_path.display(),
                    err
                );
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    };

    let mut db = db::new(&db_path).unwrap_or_else(|err| {
        eprintln!("Failed to open {}: {}", db_path.display(), err);
        exit(1)
    });

    match app_matches.subcommand() {
        ("price", Some(arg_matches)) => {
            let when = value_t!(arg_matches, "when", String)
                .map(|s| naivedate_of(&s).unwrap())
                .ok();
            let token = MaybeToken::from(value_t!(arg_matches, "token", Token).ok());

            let (price, verbose_msg) = if let Some(when) = when {
                (
                    token.get_historical_price(rpc_client, when).await?,
                    format!("Historical {token} price on {when}"),
                )
            } else {
                (
                    token.get_current_price(rpc_client).await?,
                    format!("Current {token} price"),
                )
            };

            if verbose {
                println!("{verbose_msg}: ${price:.6}");

                if let Some(liquidity_token) = token.liquidity_token() {
                    let rate = token.get_current_liquidity_token_rate(rpc_client).await?;
                    println!(
                        "Liquidity token: {} (rate: {}, inv: {})",
                        liquidity_token,
                        rate,
                        Decimal::from_usize(1).unwrap() / rate
                    );
                }
            } else {
                println!("{price:.6}");
            }
        }
        ("sync", Some(arg_matches)) => {
            let max_epochs_to_process = value_t!(arg_matches, "max_epochs_to_process", u64).ok();
            process_sync_swaps(&mut db, rpc_client, &notifier).await?;
            for (exchange, exchange_credentials, exchange_account) in
                db.get_default_accounts_from_configured_exchanges()
            {
                println!("Synchronizing {exchange:?} {exchange_account}...");
                let exchange_client = exchange_client_new(exchange, exchange_credentials)?;
                process_sync_exchange(
                    &mut db,
                    exchange,
                    exchange_client.as_ref(),
                    rpc_client,
                    &notifier,
                )
                .await?
            }
            process_account_sync(
                &mut db,
                &rpc_clients,
                None,
                max_epochs_to_process,
                false,
                false,
                &notifier,
            )
            .await?;
        }
        ("db", Some(db_matches)) => match db_matches.subcommand() {
            ("import", Some(arg_matches)) => {
                let other_db_path = value_t_or_exit!(arg_matches, "other_db_path", PathBuf);

                let mut other_db_fd_lock =
                    fd_lock::RwLock::new(fs::File::open(&other_db_path).unwrap());
                let _other_db_write_lock = loop {
                    match other_db_fd_lock.try_write() {
                        Ok(lock) => break lock,
                        Err(err) => {
                            eprintln!(
                                "Unable to lock database directory: {}: {}",
                                other_db_path.display(),
                                err
                            );
                            std::thread::sleep(std::time::Duration::from_secs(1));
                        }
                    }
                };

                let other_db = db::new(&other_db_path).unwrap_or_else(|err| {
                    eprintln!("Failed to open {}: {}", other_db_path.display(), err);
                    exit(1)
                });

                println!("Importing {}", other_db_path.display());
                db.import_db(other_db)?;
            }
            _ => unreachable!(),
        },
        ("influxdb", Some(db_matches)) => match db_matches.subcommand() {
            ("clear", Some(_arg_matches)) => {
                db.clear_metrics_config()?;
                println!("Cleared InfluxDb configuration");
            }
            ("show", Some(_arg_matches)) => match db.get_metrics_config() {
                None => {
                    println!("No InfluxDb configuration");
                }
                Some(MetricsConfig {
                    url,
                    token: _,
                    org,
                    bucket,
                }) => {
                    println!("Url: {url}");
                    println!("Token: ********");
                    println!("Organization: {org}");
                    println!("Bucket: {bucket}");
                }
            },
            ("set", Some(arg_matches)) => {
                db.set_metrics_config(MetricsConfig {
                    url: value_t_or_exit!(arg_matches, "url", String),
                    token: value_t_or_exit!(arg_matches, "token", String),
                    org: value_t_or_exit!(arg_matches, "org", String),
                    bucket: value_t_or_exit!(arg_matches, "bucket", String),
                })?;
                println!("InfluxDb configuration set");
            }
            _ => unreachable!(),
        },
        ("account", Some(account_matches)) => match account_matches.subcommand() {
            ("lot", Some(lot_matches)) => match lot_matches.subcommand() {
                ("swap", Some(arg_matches)) => {
                    let lot_number1 = value_t_or_exit!(arg_matches, "lot_number1", usize);
                    let lot_number2 = value_t_or_exit!(arg_matches, "lot_number2", usize);
                    println!("Swapping lots {lot_number1} and {lot_number2}");
                    db.swap_lots(lot_number1, lot_number2)?;
                }
                ("collect", Some(arg_matches)) => {
                    let address = pubkey_of(arg_matches, "address").unwrap();
                    let token = MaybeToken::from(value_t!(arg_matches, "token", Token).ok());
                    let lot_selection_method =
                        value_t_or_exit!(arg_matches, "lot_selection", LotSelectionMethod);

                    println!(
                        "Collecting {lot_selection_method:?} lots for {address} ({})",
                        token.name()
                    );
                    loop {
                        let mut current_lots = vec![];
                        let mut candidate_lots = vec![];
                        for account in db.get_accounts() {
                            if (account.token == token)
                                || (token.is_sol_or_wsol() && account.token.is_sol_or_wsol())
                            {
                                if account.address == address && account.token == token {
                                    assert!(current_lots.is_empty());
                                    current_lots = account.lots;
                                } else {
                                    candidate_lots.extend(account.lots);
                                }
                            }
                        }

                        sort_lots_by_selection_method(&mut current_lots, lot_selection_method);
                        sort_lots_by_selection_method(&mut candidate_lots, lot_selection_method);

                        while !current_lots.is_empty() && !candidate_lots.is_empty() {
                            if lot_selection_method.cmp_lots(&current_lots[0], &candidate_lots[0])
                                == std::cmp::Ordering::Greater
                            {
                                break;
                            }
                            current_lots.remove(0);
                        }

                        if current_lots.is_empty() || candidate_lots.is_empty() {
                            println!("Done");
                            break;
                        }

                        println!(
                            "Swapping lots {} and {}",
                            current_lots[0].lot_number, candidate_lots[0].lot_number
                        );
                        db.swap_lots(current_lots[0].lot_number, candidate_lots[0].lot_number)?;
                    }
                }
                ("move", Some(arg_matches)) => {
                    let lot_number = value_t_or_exit!(arg_matches, "lot_number", usize);
                    let to_address =
                        pubkey_of_signer(arg_matches, "to_address", &mut wallet_manager)?
                            .expect("to");
                    db.move_lot(lot_number, to_address)?;
                }
                ("delete", Some(arg_matches)) => {
                    let lot_numbers = lot_numbers_of(arg_matches, "lot_numbers").unwrap();
                    let confirm = arg_matches.is_present("confirm");

                    if !confirm {
                        println!("Add --confirm to remove lot {lot_numbers:?}");
                        return Ok(());
                    }
                    for lot_number in lot_numbers {
                        db.delete_lot(lot_number)?;
                    }
                }
                _ => unreachable!(),
            },
            ("add", Some(arg_matches)) => {
                let price = value_t!(arg_matches, "price", f64).ok();
                let income = arg_matches.is_present("income");
                let when = value_t!(arg_matches, "when", String)
                    .map(|s| naivedate_of(&s).unwrap())
                    .ok();
                let signature = value_t!(arg_matches, "transaction", Signature).ok();
                let address = pubkey_of(arg_matches, "address").unwrap();
                let token = value_t!(arg_matches, "token", Token).ok();
                let description = value_t!(arg_matches, "description", String)
                    .ok()
                    .unwrap_or_default();
                let no_sync = arg_matches.is_present("no_sync");
                let ui_amount = value_t!(arg_matches, "amount", f64).ok();
                let ui_negative_amount = value_t!(arg_matches, "neg_amount", f64).ok();

                process_account_add(
                    &mut db,
                    rpc_client,
                    address,
                    token.into(),
                    description,
                    when,
                    price,
                    income,
                    signature,
                    no_sync,
                    ui_amount,
                    ui_negative_amount,
                )
                .await?;
                process_account_sync(
                    &mut db,
                    &rpc_clients,
                    Some(address),
                    None,
                    false,
                    false,
                    &notifier,
                )
                .await?;
            }
            ("dispose", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address").unwrap();
                let token = value_t!(arg_matches, "token", Token).ok();
                let amount = value_t_or_exit!(arg_matches, "amount", f64);
                let description = value_t!(arg_matches, "description", String)
                    .ok()
                    .unwrap_or_default();
                let when = value_t!(arg_matches, "when", String)
                    .map(|s| naivedate_of(&s).unwrap())
                    .ok();
                let price = value_t!(arg_matches, "price", f64).ok();
                let lot_numbers = lot_numbers_of(arg_matches, "lot_numbers");
                let lot_selection_method =
                    value_t_or_exit!(arg_matches, "lot_selection", LotSelectionMethod);

                process_account_dispose(
                    &mut db,
                    rpc_client,
                    address,
                    token.into(),
                    amount,
                    description,
                    when,
                    price,
                    lot_selection_method,
                    lot_numbers,
                )
                .await?;
            }
            ("drop", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address").unwrap();
                let token = MaybeToken::from(value_t!(arg_matches, "token", Token).ok());
                let ui_amount = value_t_or_exit!(arg_matches, "amount", f64);
                let lot_numbers = lot_numbers_of(arg_matches, "lot_numbers");
                let lot_selection_method =
                    value_t_or_exit!(arg_matches, "lot_selection", LotSelectionMethod);
                let confirm = arg_matches.is_present("confirm");

                if !confirm {
                    println!(
                        "Add --confirm to drop {} from {} ({})",
                        token.format_ui_amount(ui_amount),
                        address,
                        token
                    );
                    return Ok(());
                }

                db.record_drop(
                    address,
                    token,
                    token.amount(ui_amount),
                    lot_selection_method,
                    lot_numbers,
                )?;
            }
            ("ls", Some(arg_matches)) => {
                let all = arg_matches.is_present("all");
                let summary = arg_matches.is_present("summary");
                let account_filter = pubkey_of(arg_matches, "account");
                process_account_list(
                    &db,
                    rpc_client,
                    account_filter,
                    all,
                    summary,
                    &notifier,
                    verbose,
                )
                .await?;
            }
            ("cost-basis", Some(arg_matches)) => {
                let when = value_t!(arg_matches, "when", String)
                    .map(|s| naivedate_of(&s).unwrap())
                    .unwrap();

                process_account_cost_basis(&db, when).await?;
            }
            ("xls", Some(arg_matches)) => {
                let outfile = value_t_or_exit!(arg_matches, "outfile", String);
                let filter_by_year = value_t!(arg_matches, "year", i32).ok();
                process_account_xls(&db, &outfile, filter_by_year).await?;
            }
            ("csv", Some(arg_matches)) => {
                let outfile = value_t_or_exit!(arg_matches, "outfile", String);
                let filter_by_year = value_t!(arg_matches, "year", i32).ok();
                process_account_csv(&db, &outfile, filter_by_year).await?;
            }
            ("remove", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address").unwrap();
                let token = MaybeToken::from(value_t!(arg_matches, "token", Token).ok());
                let confirm = arg_matches.is_present("confirm");
                let proceed_even_if_lots_exist =
                    arg_matches.is_present("proceed_even_if_lots_exist");

                let account = db
                    .get_account(address, token)
                    .ok_or_else(|| format!("Account {address} ({token}) does not exist"))?;
                if !account.lots.is_empty() {
                    if proceed_even_if_lots_exist {
                        println!(
                            "Account {address} ({token}) is not empty.\
                              Proceeding due to --proceed-even-if-lots-exist flag"
                        );
                    } else {
                        return Err(format!("Account {address} ({token}) is not empty").into());
                    }
                }

                if confirm {
                    db.remove_account(address, token)?;
                    println!("Removed {address} ({token})");
                } else {
                    println!("Add --confirm to remove {address} ({token})");
                }
            }
            ("set-sweep-stake-account", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address").unwrap();
                let stake_authority = std::fs::canonicalize(value_t_or_exit!(
                    arg_matches,
                    "stake_authority",
                    PathBuf
                ))?;

                let sweep_stake_authority_keypair =
                    read_keypair_file(&stake_authority).map_err(|err| {
                        format!("Failed to read {}: {}", stake_authority.display(), err)
                    })?;
                let (sweep_stake_authorized, _vote_account_address) =
                    rpc_client_utils::get_stake_authorized(rpc_client, address)?;

                if sweep_stake_authorized.staker != sweep_stake_authority_keypair.pubkey() {
                    return Err("Stake authority mismatch".into());
                }

                db.set_sweep_stake_account(SweepStakeAccount {
                    address,
                    stake_authority,
                })?;

                println!("Sweep stake account set to {address}");
            }
            ("set-tax-rate", Some(arg_matches)) => {
                let income = arg_matches
                    .value_of("income")
                    .unwrap()
                    .parse::<f64>()
                    .unwrap();
                let short_term_gain = arg_matches
                    .value_of("short-term-gain")
                    .unwrap()
                    .parse::<f64>()
                    .unwrap();
                let long_term_gain = arg_matches
                    .value_of("long-term-gain")
                    .map(|x| x.parse::<f64>().unwrap())
                    .unwrap_or(short_term_gain);

                println!("Income tax rate: {income:.2}");
                println!("Short-term gain rate: {short_term_gain:.2}");
                println!("Long-term gain rate: {long_term_gain:.2}");

                db.set_tax_rate(TaxRate {
                    income,
                    short_term_gain,
                    long_term_gain,
                })?;
            }
            ("tax-rate", Some(_arg_matches)) => {
                if let Some(TaxRate {
                    income,
                    short_term_gain,
                    long_term_gain,
                }) = db.get_tax_rate()
                {
                    println!("Income tax rate: {income:.2}");
                    println!("Short-term gain rate: {short_term_gain:.2}");
                    println!("Long-term gain rate: {long_term_gain:.2}");
                } else {
                    println!("(unset)");
                }
            }
            ("merge", Some(arg_matches)) => {
                let from_address = pubkey_of(arg_matches, "from_address").unwrap();
                let into_address = pubkey_of(arg_matches, "into_address").unwrap();

                let (authority_signer, authority_address) = if arg_matches.is_present("by") {
                    signer_of(arg_matches, "by", &mut wallet_manager)?
                } else {
                    signer_of(arg_matches, "from_address", &mut wallet_manager).map_err(|err| {
                        format!("Authority not found, consider using the `--by` argument): {err}")
                    })?
                };

                let authority_address = authority_address.expect("authority_address");
                let authority_signer = authority_signer.expect("authority_signer");
                let signature = value_t!(arg_matches, "transaction", Signature).ok();

                process_account_merge(
                    &mut db,
                    &rpc_clients,
                    from_address,
                    into_address,
                    authority_address,
                    vec![authority_signer],
                    priority_fee,
                    signature,
                )
                .await?;
            }
            ("sweep", Some(arg_matches)) => {
                let token = value_t!(arg_matches, "token", Token).ok().into();
                let from_address = pubkey_of(arg_matches, "address").unwrap();
                let (from_authority_signer, from_authority_address) =
                    signer_of(arg_matches, "authority", &mut wallet_manager)?;
                let from_authority_address = from_authority_address.expect("authority_address");
                let from_authority_signer = from_authority_signer.expect("authority_signer");
                let retain_ui_amount = value_t!(arg_matches, "retain", f64).unwrap_or(0.);
                let exactly_ui_amount = value_t!(arg_matches, "exactly", f64).ok();
                let no_sweep_ok = arg_matches.is_present("no_sweep_ok");
                let to_address = pubkey_of(arg_matches, "to");
                let signature = value_t!(arg_matches, "transaction", Signature).ok();

                process_account_sweep(
                    &mut db,
                    &rpc_clients,
                    from_address,
                    token,
                    token.amount(retain_ui_amount),
                    exactly_ui_amount.map(|ui_amount| token.amount(ui_amount)),
                    no_sweep_ok,
                    from_authority_address,
                    vec![from_authority_signer],
                    to_address,
                    &notifier,
                    priority_fee,
                    signature,
                )
                .await?;
            }
            ("split", Some(arg_matches)) => {
                let from_address = pubkey_of(arg_matches, "from_address").unwrap();
                let amount = match arg_matches.value_of("amount").unwrap() {
                    "ALL" => None,
                    amount => Some(MaybeToken::SOL().amount(amount.parse::<f64>().unwrap())),
                };
                let description = value_t!(arg_matches, "description", String).ok();
                let lot_numbers = lot_numbers_of(arg_matches, "lot_numbers");
                let lot_selection_method =
                    value_t_or_exit!(arg_matches, "lot_selection", LotSelectionMethod);
                let into_keypair = keypair_of(arg_matches, "into_keypair");

                let (authority_signer, authority_address) = if arg_matches.is_present("by") {
                    signer_of(arg_matches, "by", &mut wallet_manager)?
                } else {
                    signer_of(arg_matches, "from_address", &mut wallet_manager).map_err(|err| {
                        format!("Authority not found, consider using the `--by` argument): {err}")
                    })?
                };

                let authority_address = authority_address.expect("authority_address");
                let authority_signer = authority_signer.expect("authority_signer");
                let if_balance_exceeds = value_t!(arg_matches, "if_balance_exceeds", f64).ok();

                process_account_split(
                    &mut db,
                    &rpc_clients,
                    from_address,
                    amount,
                    description,
                    lot_selection_method,
                    lot_numbers,
                    authority_address,
                    vec![authority_signer],
                    into_keypair,
                    if_balance_exceeds,
                    priority_fee,
                )
                .await?;
            }
            ("redelegate", Some(arg_matches)) => {
                let from_address = pubkey_of(arg_matches, "from_address").unwrap();
                let vote_account_address = pubkey_of(arg_matches, "vote_account_address").unwrap();
                let lot_selection_method =
                    value_t_or_exit!(arg_matches, "lot_selection", LotSelectionMethod);
                let into_keypair = keypair_of(arg_matches, "into_keypair");

                let (authority_signer, authority_address) = if arg_matches.is_present("by") {
                    signer_of(arg_matches, "by", &mut wallet_manager)?
                } else {
                    signer_of(arg_matches, "from_address", &mut wallet_manager).map_err(|err| {
                        format!("Authority not found, consider using the `--by` argument): {err}")
                    })?
                };

                let authority_address = authority_address.expect("authority_address");
                let authority_signer = authority_signer.expect("authority_signer");

                process_account_redelegate(
                    &mut db,
                    &rpc_clients,
                    from_address,
                    vote_account_address,
                    lot_selection_method,
                    authority_address,
                    &vec![authority_signer],
                    into_keypair,
                )
                .await?;
            }
            ("sync", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address");
                let reconcile_no_sync_account_balances =
                    arg_matches.is_present("reconcile_no_sync_account_balances");
                let force_rescan_balances = arg_matches.is_present("force_rescan_balances");
                let max_epochs_to_process =
                    value_t!(arg_matches, "max_epochs_to_process", u64).ok();
                process_account_sync(
                    &mut db,
                    &rpc_clients,
                    address,
                    max_epochs_to_process,
                    reconcile_no_sync_account_balances,
                    force_rescan_balances,
                    &notifier,
                )
                .await?;
            }
            ("wrap", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address").unwrap();
                let amount = match arg_matches.value_of("amount").unwrap() {
                    "ALL" => Amount::All,
                    "HALF" => Amount::Half,
                    amount => {
                        Amount::Exact(MaybeToken::SOL().amount(amount.parse::<f64>().unwrap()))
                    }
                };
                let if_source_balance_exceeds =
                    value_t!(arg_matches, "if_source_balance_exceeds", f64)
                        .ok()
                        .map(|x| MaybeToken::SOL().amount(x));
                let lot_numbers = lot_numbers_of(arg_matches, "lot_numbers");
                let lot_selection_method =
                    value_t_or_exit!(arg_matches, "lot_selection", LotSelectionMethod);

                let (authority_signer, authority_address) = if arg_matches.is_present("by") {
                    signer_of(arg_matches, "by", &mut wallet_manager)?
                } else {
                    signer_of(arg_matches, "address", &mut wallet_manager).map_err(|err| {
                        format!("Authority not found, consider using the `--by` argument): {err}")
                    })?
                };

                let authority_address = authority_address.expect("authority_address");
                let authority_signer = authority_signer.expect("authority_signer");

                process_account_wrap(
                    &mut db,
                    &rpc_clients,
                    address,
                    amount,
                    if_source_balance_exceeds,
                    lot_selection_method,
                    lot_numbers,
                    authority_address,
                    vec![authority_signer],
                    priority_fee,
                )
                .await?;
            }
            ("unwrap", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address").unwrap();
                let amount = match arg_matches.value_of("amount").unwrap() {
                    "ALL" => None,
                    amount => Some(MaybeToken::SOL().amount(amount.parse::<f64>().unwrap())),
                };
                let lot_numbers = lot_numbers_of(arg_matches, "lot_numbers");
                let lot_selection_method =
                    value_t_or_exit!(arg_matches, "lot_selection", LotSelectionMethod);

                let (authority_signer, authority_address) = if arg_matches.is_present("by") {
                    signer_of(arg_matches, "by", &mut wallet_manager)?
                } else {
                    signer_of(arg_matches, "address", &mut wallet_manager).map_err(|err| {
                        format!("Authority not found, consider using the `--by` argument): {err}")
                    })?
                };

                let authority_address = authority_address.expect("authority_address");
                let authority_signer = authority_signer.expect("authority_signer");

                process_account_unwrap(
                    &mut db,
                    &rpc_clients,
                    address,
                    amount,
                    lot_selection_method,
                    lot_numbers,
                    authority_address,
                    vec![authority_signer],
                    priority_fee,
                )
                .await?;
            }
            _ => unreachable!(),
        },
        ("jup", Some(jup_matches)) => match jup_matches.subcommand() {
            ("quote", Some(arg_matches)) => {
                let from_token = MaybeToken::from(value_t!(arg_matches, "from_token", Token).ok());
                let to_token = MaybeToken::from(value_t!(arg_matches, "to_token", Token).ok());
                let ui_amount = value_t_or_exit!(arg_matches, "amount", f64);
                let slippage_bps = value_t_or_exit!(arg_matches, "slippage_bps", u64);

                process_jup_quote(from_token, to_token, ui_amount, slippage_bps).await?;
            }
            ("swap", Some(arg_matches)) => {
                let (signer, address) = signer_of(arg_matches, "address", &mut wallet_manager)?;
                let from_token = MaybeToken::from(value_t!(arg_matches, "from_token", Token).ok());
                let to_token = MaybeToken::from(value_t!(arg_matches, "to_token", Token).ok());
                let ui_amount = match arg_matches.value_of("amount").unwrap() {
                    "ALL" => None,
                    ui_amount => Some(ui_amount.parse::<f64>().unwrap()),
                };
                let slippage_bps = value_t_or_exit!(arg_matches, "slippage_bps", u64);
                let signer = signer.expect("signer");
                let address = address.expect("address");
                let lot_selection_method =
                    value_t_or_exit!(arg_matches, "lot_selection", LotSelectionMethod);
                let signature = value_t!(arg_matches, "transaction", Signature).ok();
                let if_from_balance_exceeds = value_t!(arg_matches, "if_from_balance_exceeds", f64)
                    .ok()
                    .map(|x| from_token.amount(x));
                let for_no_less_than = value_t!(arg_matches, "for_no_less_than", f64).ok();
                let max_coingecko_value_percentage_loss =
                    value_t_or_exit!(arg_matches, "max_coingecko_value_percentage_loss", f64);

                process_jup_swap(
                    &mut db,
                    &rpc_clients,
                    address,
                    from_token,
                    to_token,
                    ui_amount,
                    slippage_bps,
                    lot_selection_method,
                    vec![signer],
                    signature,
                    if_from_balance_exceeds,
                    for_no_less_than,
                    max_coingecko_value_percentage_loss,
                    priority_fee,
                    &notifier,
                )
                .await?;
                process_sync_swaps(&mut db, rpc_client, &notifier).await?;
            }
            _ => unreachable!(),
        },
        (exchange, Some(exchange_matches)) => {
            assert!(exchanges.contains(&exchange), "Bug!");
            let exchange = Exchange::from_str(exchange)?;

            let exchange_account = value_t!(exchange_matches, "exchange_account", String)
                .ok()
                .unwrap_or_default();

            let exchange_client = || {
                let exchange_credentials = db
                    .get_exchange_credentials(exchange, &exchange_account)
                    .ok_or_else(|| format!("No API key set for {exchange:?}"))?;
                exchange_client_new(exchange, exchange_credentials)
            };

            match exchange_matches.subcommand() {
                ("address", Some(arg_matches)) => {
                    let token = MaybeToken::from(value_t!(arg_matches, "token", Token).ok());
                    let deposit_address = exchange_client()?.deposit_address(token).await?;
                    println!("{token} deposit address: {deposit_address}");
                }
                ("pending-deposits", Some(arg_matches)) => {
                    let quiet = arg_matches.is_present("quiet");

                    let pending_deposits = db.pending_deposits(Some(exchange));
                    if quiet {
                        if !pending_deposits.is_empty() {
                            return Err(
                                format!("{} deposits pending", pending_deposits.len()).into()
                            );
                        }
                    } else {
                        for pending_deposit in pending_deposits {
                            let token = pending_deposit.transfer.to_token;
                            assert_eq!(
                                pending_deposit.transfer.from_token,
                                pending_deposit.transfer.to_token
                            );

                            println!(
                                "{} deposit pending: {}{} (signature: {})",
                                token,
                                token.symbol(),
                                token.ui_amount(pending_deposit.amount),
                                pending_deposit.transfer.signature
                            );
                        }
                    }
                }
                ("pending-withdrawals", Some(arg_matches)) => {
                    let quiet = arg_matches.is_present("quiet");

                    let pending_withdrawals = db.pending_withdrawals(Some(exchange));
                    if quiet {
                        if !pending_withdrawals.is_empty() {
                            return Err(format!(
                                "{} withdrawals pending",
                                pending_withdrawals.len()
                            )
                            .into());
                        }
                    } else {
                        for pending_withdrawals in pending_withdrawals {
                            let token = pending_withdrawals.token;
                            println!(
                                "{} withdrawal pending: {}{} (destination: {})",
                                token,
                                token.symbol(),
                                token.ui_amount(pending_withdrawals.amount),
                                pending_withdrawals.to_address,
                            );
                        }
                    }
                }
                ("balance", Some(arg_matches)) => {
                    let available_only = arg_matches.is_present("available_only");
                    let total_only = arg_matches.is_present("total_only");
                    let integer = arg_matches.is_present("integer");

                    let balances = exchange_client()?.balances().await?;

                    if !(available_only || total_only) {
                        println!("                   Total            Available")
                    }

                    let balance = balances.get("SOL").cloned().unwrap_or_default();

                    let print_balance = |coin: &str, symbol: &str, balance: &ExchangeBalance| {
                        let symbol = if integer { "" } else { symbol };
                        let available_balance = format!(
                            "{}{}",
                            symbol,
                            if integer {
                                balance.available.floor().to_string()
                            } else {
                                balance.available.separated_string_with_fixed_place(8)
                            }
                        );

                        let total_balance = format!(
                            "{}{}",
                            symbol,
                            if integer {
                                balance.total.floor().to_string()
                            } else {
                                balance.total.separated_string_with_fixed_place(8)
                            }
                        );

                        if available_only {
                            println!("{coin} {available_balance}");
                        } else if total_only {
                            println!("{coin} {total_balance}");
                        } else {
                            println!("{coin} {total_balance:>20} {available_balance:>20}");
                        }
                    };

                    print_balance("SOL", "◎", &balance);
                    for coin in exchange::USD_COINS {
                        if let Some(balance) = balances.get(*coin) {
                            if balance.total > 0. {
                                print_balance(coin, "$", balance);
                            }
                        }
                    }
                }
                ("market", Some(arg_matches)) => {
                    let exchange_client = exchange_client()?;

                    let pair = value_t!(arg_matches, "pair", String)
                        .unwrap_or_else(|_| exchange_client.preferred_solusd_pair().into());
                    let format = if arg_matches.is_present("weighted_24h_average_price") {
                        MarketInfoFormat::Weighted24hAveragePrice
                    } else if arg_matches.is_present("hourly") {
                        MarketInfoFormat::Hourly
                    } else if arg_matches.is_present("ask") {
                        MarketInfoFormat::Ask
                    } else {
                        MarketInfoFormat::All
                    };
                    exchange_client.print_market_info(&pair, format).await?;
                }
                ("deposit", Some(arg_matches)) => {
                    let token = MaybeToken::from(value_t!(arg_matches, "token", Token).ok());
                    let amount = match arg_matches.value_of("amount").unwrap() {
                        "ALL" => Amount::All,
                        "HALF" => Amount::Half,
                        amount => Amount::Exact(token.amount(amount.parse().unwrap())),
                    };
                    let if_source_balance_exceeds =
                        value_t!(arg_matches, "if_source_balance_exceeds", f64)
                            .ok()
                            .map(|x| token.amount(x));
                    let if_exchange_balance_less_than =
                        value_t!(arg_matches, "if_exchange_balance_less_than", f64)
                            .ok()
                            .map(|x| token.amount(x));
                    let from_address =
                        pubkey_of_signer(arg_matches, "from", &mut wallet_manager)?.expect("from");
                    let lot_numbers = lot_numbers_of(arg_matches, "lot_numbers");
                    let lot_selection_method =
                        value_t_or_exit!(arg_matches, "lot_selection", LotSelectionMethod);

                    let (authority_signer, authority_address) = if arg_matches.is_present("by") {
                        signer_of(arg_matches, "by", &mut wallet_manager)?
                    } else {
                        signer_of(arg_matches, "from", &mut wallet_manager).map_err(|err| {
                            format!(
                                "Authority not found, consider using the `--by` argument): {err}"
                            )
                        })?
                    };

                    let authority_address = authority_address.expect("authority_address");
                    let authority_signer = authority_signer.expect("authority_signer");

                    let exchange_client = exchange_client()?;
                    let deposit_address = exchange_client.deposit_address(token).await?;
                    add_exchange_deposit_address_to_db(
                        &mut db,
                        exchange,
                        &exchange_account,
                        token,
                        deposit_address,
                        rpc_client,
                    )?;
                    process_exchange_deposit(
                        &mut db,
                        &rpc_clients,
                        exchange,
                        exchange_client.as_ref(),
                        token,
                        deposit_address,
                        amount,
                        from_address,
                        if_source_balance_exceeds,
                        if_exchange_balance_less_than,
                        authority_address,
                        vec![authority_signer],
                        lot_selection_method,
                        lot_numbers,
                        priority_fee,
                    )
                    .await?;
                    process_sync_exchange(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        rpc_client,
                        &notifier,
                    )
                    .await?;
                }
                ("withdraw", Some(arg_matches)) => {
                    let token = MaybeToken::from(value_t!(arg_matches, "token", Token).ok());
                    let amount = match arg_matches.value_of("amount").unwrap() {
                        "ALL" => None,
                        amount => Some(token.amount(amount.parse().unwrap())),
                    };
                    let to_address =
                        pubkey_of_signer(arg_matches, "to", &mut wallet_manager)?.expect("to");
                    let lot_numbers = lot_numbers_of(arg_matches, "lot_numbers");
                    let lot_selection_method =
                        value_t_or_exit!(arg_matches, "lot_selection", LotSelectionMethod);

                    let withdrawal_password = None; // TODO: Support reading password from stdin
                    let withdrawal_code = value_t!(arg_matches, "code", String).ok();

                    let exchange_client = exchange_client()?;
                    let deposit_address = exchange_client.deposit_address(token).await?;
                    add_exchange_deposit_address_to_db(
                        &mut db,
                        exchange,
                        &exchange_account,
                        token,
                        deposit_address,
                        rpc_client,
                    )?;

                    process_exchange_withdraw(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        token,
                        deposit_address,
                        amount,
                        to_address,
                        lot_selection_method,
                        lot_numbers,
                        withdrawal_password,
                        withdrawal_code,
                    )
                    .await?;
                    process_sync_exchange(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        rpc_client,
                        &notifier,
                    )
                    .await?;
                }
                ("cancel", Some(arg_matches)) => {
                    let order_ids: HashSet<String> = values_t!(arg_matches, "order_id", String)
                        .ok()
                        .map(|x| x.into_iter().collect())
                        .unwrap_or_default();

                    let max_create_time = value_t!(arg_matches, "age", i64).ok().and_then(|age| {
                        Utc::now().checked_sub_signed(chrono::Duration::try_hours(age).unwrap())
                    });

                    let side = value_t_or_exit!(arg_matches, "side", String);
                    let side = match side.as_str() {
                        "buy" => Some(OrderSide::Buy),
                        "sell" => Some(OrderSide::Sell),
                        "both" => None,
                        _ => unreachable!(),
                    };

                    let exchange_client = exchange_client()?;
                    process_exchange_cancel(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        order_ids,
                        max_create_time,
                        side,
                    )
                    .await?;

                    process_sync_exchange(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        rpc_client,
                        &notifier,
                    )
                    .await?;
                }
                ("buy", Some(arg_matches)) => {
                    let exchange_client = exchange_client()?;
                    let token = MaybeToken::SOL();
                    let pair = value_t!(arg_matches, "pair", String)
                        .unwrap_or_else(|_| exchange_client.preferred_solusd_pair().into());
                    let amount = match arg_matches.value_of("amount").unwrap() {
                        "ALL" => None,
                        amount => Some(str::parse::<f64>(amount).unwrap()),
                    };

                    let if_balance_exceeds = value_t!(arg_matches, "if_balance_exceeds", f64).ok();

                    let price = if let Ok(price) = value_t!(arg_matches, "at", f64) {
                        LimitOrderPrice::At(price)
                    } else if let Ok(bid_minus) = value_t!(arg_matches, "bid_minus", f64) {
                        LimitOrderPrice::AmountUnderBid(bid_minus)
                    } else {
                        return Err("--at or --bid-minus argument required".into());
                    };

                    process_exchange_buy(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        token,
                        pair,
                        amount,
                        price,
                        if_balance_exceeds,
                        &notifier,
                    )
                    .await?;
                    process_sync_exchange(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        rpc_client,
                        &notifier,
                    )
                    .await?;
                }
                ("sell", Some(arg_matches)) => {
                    let exchange_client = exchange_client()?;
                    let token = MaybeToken::SOL();
                    let pair = value_t!(arg_matches, "pair", String)
                        .unwrap_or_else(|_| exchange_client.preferred_solusd_pair().into());
                    let amount = value_t_or_exit!(arg_matches, "amount", f64);
                    let if_balance_exceeds = value_t!(arg_matches, "if_balance_exceeds", f64)
                        .ok()
                        .map(|x| token.amount(x));
                    let if_price_over = value_t!(arg_matches, "if_price_over", f64).ok();
                    let if_price_over_basis = arg_matches.is_present("if_price_over_basis");
                    let price_floor = value_t!(arg_matches, "price_floor", f64).ok();
                    let lot_numbers = lot_numbers_of(arg_matches, "lot_numbers");
                    let lot_selection_method =
                        value_t_or_exit!(arg_matches, "lot_selection", LotSelectionMethod);

                    let price = if let Ok(price) = value_t!(arg_matches, "at", f64) {
                        LimitOrderPrice::At(price)
                    } else if let Ok(ask_plus) = value_t!(arg_matches, "ask_plus", f64) {
                        LimitOrderPrice::AmountOverAsk(ask_plus)
                    } else {
                        return Err("--at or --ask-plus argument required".into());
                    };
                    process_exchange_sell(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        token,
                        pair,
                        amount,
                        price,
                        if_balance_exceeds,
                        if_price_over,
                        if_price_over_basis,
                        price_floor,
                        lot_selection_method,
                        lot_numbers,
                        &notifier,
                    )
                    .await?;
                    process_sync_exchange(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        rpc_client,
                        &notifier,
                    )
                    .await?;
                }
                ("lend", Some(arg_matches)) => {
                    let coin = value_t_or_exit!(arg_matches, "coin", String);
                    let amount = arg_matches.value_of("amount");
                    let available = arg_matches.is_present("available");

                    let exchange_client = exchange_client()?;

                    let lending_info = exchange_client
                        .get_lending_info(&coin)
                        .await?
                        .ok_or_else(|| format!("Lending not available for {coin}"))?;

                    if let Some(amount) = amount {
                        let amount = if available {
                            if amount == "ALL" {
                                0.
                            } else {
                                lending_info.lendable - amount.parse::<f64>().unwrap()
                            }
                        } else if amount == "ALL" {
                            lending_info.lendable
                        } else {
                            amount.parse::<f64>().unwrap()
                        }
                        .floor()
                        .max(0.);

                        let additional_amount = amount - lending_info.offered;
                        if additional_amount.abs() > f64::EPSILON {
                            let msg = format!(
                                "Lending offer: {} {} (change: {}) at {:.1}%",
                                amount.separated_string_with_fixed_place(2),
                                coin,
                                additional_amount.separated_string_with_fixed_place(2),
                                lending_info.estimate_rate,
                            );
                            exchange_client.submit_lending_offer(&coin, amount).await?;
                            println!("{msg}");
                            notifier.send(&format!("{exchange:?}: {msg}")).await;
                        } else {
                            println!(
                                "Lending offer unchanged: {}",
                                lending_info.offered.separated_string_with_fixed_place(2)
                            );
                        }
                    } else {
                        println!(
                            "Available:     {}",
                            lending_info.lendable.separated_string_with_fixed_place(2),
                        );
                        println!(
                            "Current offer: {}",
                            lending_info.offered.separated_string_with_fixed_place(2),
                        );
                        println!(
                            "Locked:        {}",
                            lending_info.locked.separated_string_with_fixed_place(2),
                        );
                        println!(
                            "Current rate:  {:.1}% (estimated)",
                            lending_info.estimate_rate
                        );
                        println!("Previous rate: {:.1}%", lending_info.previous_rate);
                    }
                }
                ("lending-history", Some(lending_history_matches)) => {
                    let exchange_client = exchange_client()?;
                    let lending_history = match lending_history_matches.subcommand() {
                        ("range", Some(arg_matches)) => {
                            let start_date =
                                naivedate_of(&value_t_or_exit!(arg_matches, "start_date", String))
                                    .unwrap();
                            let end_date =
                                naivedate_of(&value_t_or_exit!(arg_matches, "end_date", String))
                                    .unwrap();
                            exchange_client.get_lending_history(LendingHistory::Range {
                                start_date,
                                end_date,
                            })
                        }
                        ("previous", Some(arg_matches)) => {
                            let days = value_t_or_exit!(arg_matches, "days", usize);
                            exchange_client.get_lending_history(LendingHistory::Previous { days })
                        }
                        _ => unreachable!(),
                    }
                    .await?;

                    for (coin, amount) in lending_history.iter() {
                        println!("{}: {}", coin, amount.separated_string_with_fixed_place(2));
                    }
                }
                ("sync", Some(_arg_matches)) => {
                    let exchange_client = exchange_client()?;
                    process_sync_exchange(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        rpc_client,
                        &notifier,
                    )
                    .await?;
                }
                ("api", Some(api_matches)) => {
                    match api_matches.subcommand() {
                        ("show", Some(_arg_matches)) => {
                            match db.get_exchange_credentials(exchange, &exchange_account) {
                                Some(ExchangeCredentials {
                                    api_key,
                                    subaccount,
                                    ..
                                }) => {
                                    println!("Account name: {exchange_account}");
                                    println!("API Key: {api_key}");
                                    println!("Secret: ********");
                                    if let Some(subaccount) = subaccount {
                                        println!("Subaccount: {subaccount}");
                                    }
                                }
                                None => {
                                    println!("No API key set for {exchange:?}, account name: '{exchange_account}'");
                                }
                            }
                        }
                        ("set", Some(arg_matches)) => {
                            let api_key = value_t_or_exit!(arg_matches, "api_key", String);
                            let secret = value_t_or_exit!(arg_matches, "secret", String);
                            let subaccount = value_t!(arg_matches, "subaccount", String).ok();
                            db.set_exchange_credentials(
                                exchange,
                                &exchange_account,
                                ExchangeCredentials {
                                    api_key,
                                    secret,
                                    subaccount,
                                },
                            )?;
                            println!(
                                "API key set for {exchange:?}, account name: '{exchange_account}'"
                            );
                        }
                        ("clear", Some(_arg_matches)) => {
                            db.clear_exchange_credentials(exchange, &exchange_account)?;
                            println!("Cleared API key for {exchange:?}, account name: '{exchange_account}'");
                        }
                        _ => unreachable!(),
                    }
                }

                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    };

    metrics::send(db.get_metrics_config()).await;
    Ok(())
}
