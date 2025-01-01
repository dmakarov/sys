use {
    crate::{db::*, exchange::*, notifier::*, token::*, *},
    chrono::prelude::*,
    chrono_humanize::HumanTime,
    console::style,
    rust_decimal::prelude::*,
    separator::FixedPlaceSeparatable,
    solana_sdk::pubkey::Pubkey,
    std::collections::BTreeMap,
};

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

pub struct Commands {
    rpc_clients: RpcClients,
    verbose: bool,
}

impl Commands {
    pub fn new(
        json_rpc_url: String,
        send_json_rpc_urls: Option<String>,
        helius: Option<String>,
        verbose: bool,
    ) -> Self {
        Self {
            rpc_clients: RpcClients::new(json_rpc_url, send_json_rpc_urls, helius),
            verbose,
        }
    }

    pub async fn price(
        &self,
        token: Option<Token>,
        when: Option<NaiveDate>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let token = MaybeToken::from(token);
        let rpc_client = self.rpc_clients.default();
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
        if self.verbose {
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
        Ok(())
    }

    pub async fn process_account_list(
        &self,
        db: &Db,
        account_filter: Option<Pubkey>,
        show_all_lots: bool,
        summary_only: bool,
        notifier: &Notifier,
        verbose: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let rpc_client = self.rpc_clients.default();
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

                if let std::collections::btree_map::Entry::Vacant(e) =
                    held_tokens.entry(account.token)
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

            println!("Current Holdings");
            let mut held_tokens = held_tokens
                .into_iter()
                .map(
                    |(held_token, (current_token_price, total_held_amount, unrealized_gain))| {
                        let total_value = current_token_price.map(|current_token_price| {
                            f64::try_from(
                                Decimal::from_f64(held_token.ui_amount(total_held_amount)).unwrap()
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
            held_tokens.sort_unstable_by(|a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });

            for (
                held_token,
                total_value,
                current_token_price,
                total_held_amount,
                unrealized_gain,
            ) in held_tokens
            {
                if total_held_amount == 0 {
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
                        held_token.format_amount(total_held_amount)
                    );
                } else {
                    println!(
                        "  {:<7}       {:<20} [{}; ${:>4} per {:>4}{}]",
                        held_token.to_string(),
                        held_token.format_amount(total_held_amount),
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

fn format_order_side(order_side: OrderSide) -> String {
    match order_side {
        OrderSide::Buy => style(" Buy").green(),
        OrderSide::Sell => style("Sell").red(),
    }
    .to_string()
}

fn is_long_term_cap_gain(acquisition: NaiveDate, disposal: Option<NaiveDate>) -> bool {
    let disposal = disposal.unwrap_or_else(today);
    let hold_time = disposal - acquisition;
    hold_time >= chrono::Duration::try_days(365).unwrap()
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

pub(crate) fn today() -> NaiveDate {
    let today = Local::now().date_naive();
    NaiveDate::from_ymd_opt(today.year(), today.month(), today.day()).unwrap()
}
