use {
    crate::{exchange::*, token::MaybeToken},
    async_trait::async_trait,
    futures::{pin_mut, stream::StreamExt},
    rust_decimal::prelude::*,
    solana_sdk::pubkey::Pubkey,
    std::collections::HashMap,
};

pub struct CoinbaseExchangeClient {
    client: coinbase_rs::Private,
}

#[async_trait]
impl ExchangeClient for CoinbaseExchangeClient {
    async fn accounts(
        &self,
    ) -> Result<Vec<AccountInfo>, Box<dyn std::error::Error>> {
        let accounts = self.client.accounts().await;
        if let Err(e) = accounts {
            return Err(format!("Failed to get accounts: {e}").into());
        }
        Ok(
            accounts
                .unwrap()
                .iter()
                .filter(|x| x.active)
                .map(|x| AccountInfo {
                    uuid: x.uuid.clone(),
                    name: x.name.clone(),
                    currency: x.currency.clone(),
                    value: x.available_balance.value.clone(),
                })
                .collect()
        )
    }

    async fn deposit_address(
        &self,
        token: MaybeToken,
    ) -> Result<Pubkey, Box<dyn std::error::Error>> {
        let accounts = self.client.accounts().await;
        if let Err(e) = accounts {
            return Err(format!("Failed to get accounts: {e}").into());
        }

        for account in accounts.unwrap() {
            if let Ok(id) = coinbase_rs::Uuid::from_str(&account.uuid) {
                if token.name() == account.currency && account.active
                {
                    let addresses = self.client.list_addresses(&id);
                    pin_mut!(addresses);

                    let mut best_pubkey_updated_at = None;
                    let mut best_pubkey = None;
                    while let Some(addresses_result) = addresses.next().await {
                        for address in addresses_result.unwrap() {
                            if address.network.as_str() == "solana" {
                                if let Ok(pubkey) = address.address.parse::<Pubkey>() {
                                    if address.updated_at > best_pubkey_updated_at {
                                        best_pubkey_updated_at = address.updated_at;
                                        best_pubkey = Some(pubkey);
                                    }
                                }
                            }
                        }
                    }
                    if let Some(pubkey) = best_pubkey {
                        return Ok(pubkey);
                    }
                    break;
                }
            }
        }

        Err(format!("Unsupported deposit token: {}", token.name()).into())
    }

    async fn balances(
        &self,
    ) -> Result<HashMap<String, ExchangeBalance>, Box<dyn std::error::Error>> {
        Err("Balances not supported".into())
    }

    async fn recent_deposits(
        &self,
    ) -> Result<Option<Vec<DepositInfo>>, Box<dyn std::error::Error>> {
        Ok(None) // TODO: Return actual recent deposits. By returning `None`, deposited lots are dropped
                 // once the transaction is confirmed (see `db::drop_deposit()`).
    }

    async fn recent_withdrawals(&self) -> Result<Vec<WithdrawalInfo>, Box<dyn std::error::Error>> {
        Ok(vec![])
    }

    async fn request_withdraw(
        &self,
        _address: Pubkey,
        _token: MaybeToken,
        _amount: f64,
        _password: Option<String>,
        _code: Option<String>,
    ) -> Result<(/* withdraw_id: */ String, /*withdraw_fee: */ f64), Box<dyn std::error::Error>>
    {
        Err("Withdrawals not supported".into())
    }

    async fn print_market_info(
        &self,
        _pair: &str,
        _format: MarketInfoFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let public_products = self.client.list_public_products().await;
        if let Err(e) = public_products {
            return Err(format!("Failed to get public products: {e}").into());
        }
        for product in public_products.unwrap() {
            println!("{} {}", product.product_id, product.price);
        }
        Ok(())
    }

    async fn bid_ask(&self, _pair: &str) -> Result<BidAsk, Box<dyn std::error::Error>> {
        Err("Trading not supported".into())
    }

    async fn place_order(
        &self,
        _pair: &str,
        _side: OrderSide,
        _price: f64,
        _amount: f64,
    ) -> Result<OrderId, Box<dyn std::error::Error>> {
        Err("Trading not supported".into())
    }

    async fn cancel_order(
        &self,
        _pair: &str,
        _order_id: &OrderId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err("Trading not supported".into())
    }

    async fn order_status(
        &self,
        _pair: &str,
        _order_id: &OrderId,
    ) -> Result<OrderStatus, Box<dyn std::error::Error>> {
        Err("Trading not supported".into())
    }

    async fn get_lending_info(
        &self,
        _coin: &str,
    ) -> Result<Option<LendingInfo>, Box<dyn std::error::Error>> {
        Err("Lending not supported".into())
    }

    async fn get_lending_history(
        &self,
        _lending_history: LendingHistory,
    ) -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
        Err("Lending not supported".into())
    }

    async fn submit_lending_offer(
        &self,
        _coin: &str,
        _size: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err("Lending not supported".into())
    }

    async fn payment_methods(
        &self,
    ) -> Result<Vec<PaymentInfo>, Box<dyn std::error::Error>> {
        let payment_methods = self.client.list_payment_methods().await;
        if let Err(e) = payment_methods {
            return Err(format!("Failed to get payment methods: {e}").into());
        }
        Ok(
            payment_methods
                .unwrap()
                .iter()
                .filter(|x| x.allow_withdraw)
                .map(|x| PaymentInfo {
                    id: x.id.clone(),
                    r#type: x.r#type.clone(),
                    name: x.name.clone(),
                    currency: x.currency.clone(),
                })
                .collect()
        )
    }

    async fn disburse_cash(
        &self,
        account: String,
        amount: String,
        currency: String,
        method: String,
    ) -> Result<DisbursementInfo, Box<dyn std::error::Error>> {
        let transfer = self.client.withdrawals(
            account,
            amount,
            currency,
            method,
        ).await;
        if let Err(e) = transfer {
            return Err(format!("Failed to get disburse cash: {e}").into());
        }
        Ok(
            DisbursementInfo {
            }
        )
    }

    fn preferred_solusd_pair(&self) -> &'static str {
        "SOLUSD"
    }
}

pub fn new(
    ExchangeCredentials {
        api_key,
        secret,
        subaccount,
    }: ExchangeCredentials,
) -> Result<CoinbaseExchangeClient, Box<dyn std::error::Error>> {
    assert!(subaccount.is_none());
    let secret = secret.replace("\\n", "\n");
    Ok(CoinbaseExchangeClient {
        client: coinbase_rs::Private::new(coinbase_rs::MAIN_URL, &api_key, &secret),
    })
}
