use {
    crate::{token::*, *},
    chrono::prelude::*,
    rust_decimal::prelude::*,
};

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
            rpc_clients: RpcClients::new(
                json_rpc_url,
                send_json_rpc_urls,
                helius,
            ),
            verbose,
        }
    }

    pub async fn price(&self, token: Option<Token>, when: Option<NaiveDate>) -> Result<(), Box<dyn std::error::Error>> {
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
}
