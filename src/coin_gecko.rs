use {
    crate::token::{MaybeToken, Token},
    chrono::prelude::*,
    rust_decimal::prelude::*,
    serde::{Deserialize, Serialize},
    std::{collections::HashMap, env, sync::Arc},
    tokio::sync::RwLock,
};

#[derive(Debug, Serialize, Deserialize)]
struct CurrencyList {
    usd: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct MarketData {
    current_price: CurrencyList,
    market_cap: CurrencyList,
    total_volume: CurrencyList,
}

// This `derive` requires the `serde` dependency.
#[derive(Debug, Serialize, Deserialize)]
struct HistoryResponse {
    id: String,
    name: String,
    symbol: String,
    market_data: Option<MarketData>,
}

fn token_to_coin(token: &MaybeToken) -> Result<&'static str, Box<dyn std::error::Error>> {
    let coin = match token.token() {
        None => "solana",
        Some(token) => match token {
            Token::USDC => "usd-coin",
            Token::USDS => "usds",
            Token::USDT => "tether",
            Token::UXD => "uxd-stablecoin",
            Token::bSOL => "blazestake-staked-sol",
            Token::hSOL => "msol",
            Token::mSOL => "msol",
            Token::stSOL => "lido-staked-sol",
            Token::JitoSOL => "jito-staked-sol",
            Token::wSOL => "solana",
            Token::JLP => "jupiter-perpetuals-liquidity-provider-token",
            Token::JUP => "jupiter-exchange-solana",
            Token::JTO => "jito-governance-token",
            Token::BONK => "bonk",
            Token::KMNO => "kamino",
            Token::PYTH => "pyth-network",
            Token::WEN => "wen-4",
            Token::WIF => "dogwifcoin",
            Token::PYUSD => "paypal-usd",
            unsupported_token => {
                return Err(format!(
                    "Coin Gecko price data not available for {}",
                    unsupported_token.name()
                )
                .into())
            }
        },
    };
    Ok(coin)
}

fn get_cg_pro_api_key() -> (&'static str, String) {
    let (maybe_pro, x_cg_pro_api_key) = match env::var("CG_PRO_API_KEY") {
        Err(_) => ("", "".into()),
        Ok(x_cg_pro_api_key) => ("pro-", format!("&x_cg_pro_api_key={x_cg_pro_api_key}")),
    };

    (maybe_pro, x_cg_pro_api_key)
}

pub async fn get_current_price(token: &MaybeToken) -> Result<Decimal, Box<dyn std::error::Error>> {
    type CurrentPriceCache = HashMap<MaybeToken, Decimal>;
    lazy_static::lazy_static! {
        static ref CURRENT_PRICE_CACHE: Arc<RwLock<CurrentPriceCache>> = Arc::new(RwLock::new(HashMap::new()));
        static ref LAST_DATA_FETCH_INSTANT: Arc<RwLock<std::time::Instant>> = Arc::new(RwLock::new(std::time::Instant::now()));
    }
    let mut current_price_cache = CURRENT_PRICE_CACHE.write().await;
    let mut last_data_fetch_instant = LAST_DATA_FETCH_INSTANT.write().await;

    let now = std::time::Instant::now();
    if now.duration_since(*last_data_fetch_instant).as_secs() > 30 {
        current_price_cache.remove(token);
        *last_data_fetch_instant = now;
    }

    match current_price_cache.get(token) {
        Some(price) => Ok(*price),
        None => {
            let coins = Token::VARIANTS.iter().fold("solana".to_string(), |acc, x| {
                format!("{acc},{}", token_to_coin(&MaybeToken::from(*x)).unwrap())
            });
            let (maybe_pro, x_cg_pro_api_key) = get_cg_pro_api_key();
            let url = format!(
                "https://{maybe_pro}api.coingecko.com/api/v3/simple/price?ids={coins}&vs_currencies=usd{x_cg_pro_api_key}"
            );

            #[derive(Debug, Serialize, Deserialize)]
            struct Coins {
                solana: Option<CurrencyList>,
                #[serde(rename = "blazestake-staked-sol")]
                bsol: Option<CurrencyList>,
                #[serde(rename = "helius-staked-sol")]
                hsol: Option<CurrencyList>,
                msol: Option<CurrencyList>,
                #[serde(rename = "lido-staked-sol")]
                stsol: Option<CurrencyList>,
                #[serde(rename = "jito-staked-sol")]
                jitosol: Option<CurrencyList>,
                #[serde(rename = "tether")]
                tether: Option<CurrencyList>,
                #[serde(rename = "usd-coin")]
                usdc: Option<CurrencyList>,
                #[serde(rename = "usds")]
                usds: Option<CurrencyList>,
                #[serde(rename = "uxd-stablecoin")]
                uxd: Option<CurrencyList>,
                #[serde(rename = "jupiter-perpetuals-liquidity-provider-token")]
                jlp: Option<CurrencyList>,
                #[serde(rename = "jupiter-exchange-solana")]
                jup: Option<CurrencyList>,
                #[serde(rename = "jito-governance-token")]
                jto: Option<CurrencyList>,
                #[serde(rename = "bonk")]
                bonk: Option<CurrencyList>,
                #[serde(rename = "kamino")]
                kmno: Option<CurrencyList>,
                #[serde(rename = "pyth-network")]
                pyth: Option<CurrencyList>,
                #[serde(rename = "wen-4")]
                wen: Option<CurrencyList>,
                #[serde(rename = "dogwifcoin")]
                wif: Option<CurrencyList>,
                #[serde(rename = "paypal-usd")]
                pyusd: Option<CurrencyList>,
            }
            let coins = reqwest::get(url).await?.json::<Coins>().await?;
            coins.solana.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(None), price);
            });
            coins.msol.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::mSOL)), price);
            });
            coins.stsol.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::stSOL)), price);
            });
            coins.jitosol.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::JitoSOL)), price);
            });
            coins.tether.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::USDT)), price);
            });
            coins.usdc.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::USDC)), price);
            });
            coins.usds.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::USDS)), price);
            });
            coins.uxd.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::UXD)), price);
            });
            coins.bsol.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::bSOL)), price);
            });
            coins.hsol.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::hSOL)), price);
            });
            coins.jlp.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::JLP)), price);
            });
            coins.jup.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::JUP)), price);
            });
            coins.jto.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::JTO)), price);
            });
            coins.bonk.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::BONK)), price);
            });
            coins.kmno.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::KMNO)), price);
            });
            coins.pyth.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::PYTH)), price);
            });
            coins.wen.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::WEN)), price);
            });
            coins.wif.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::WIF)), price);
            });
            coins.pyusd.map(|price| {
                let price = Decimal::from_f64(price.usd).unwrap();
                current_price_cache.insert(MaybeToken::from(Some(Token::PYUSD)), price);
            });

            current_price_cache
                .get(token)
                .ok_or_else(|| format!("Simple price data not available for {token}").into())
                .copied()
        }
    }
}

pub async fn get_historical_price(
    when: NaiveDate,
    token: &MaybeToken,
) -> Result<Decimal, Box<dyn std::error::Error>> {
    type HistoricalPriceCache = HashMap<(NaiveDate, MaybeToken), Decimal>;
    lazy_static::lazy_static! {
        static ref HISTORICAL_PRICE_CACHE: Arc<RwLock<HistoricalPriceCache>> = Arc::new(RwLock::new(HashMap::new()));
    }
    let mut historical_price_cache = HISTORICAL_PRICE_CACHE.write().await;

    let price_cache_key = (when, *token);

    match historical_price_cache.get(&price_cache_key) {
        Some(price) => Ok(*price),
        None => {
            let coin = token_to_coin(token)?;

            let (maybe_pro, x_cg_pro_api_key) = get_cg_pro_api_key();
            let url = format!(
                "https://{maybe_pro}api.coingecko.com/api/v3/coins/{}/history?date={:02}-{:02}-{:4}&localization=false{x_cg_pro_api_key}",
                coin,
                when.day(),
                when.month(),
                when.year()
            );

            reqwest::get(url)
                .await?
                .json::<HistoryResponse>()
                .await?
                .market_data
                .ok_or_else(|| format!("Market data not available for {coin} on {when}").into())
                .map(|market_data| {
                    let price = Decimal::from_f64(market_data.current_price.usd).unwrap();
                    historical_price_cache.insert(price_cache_key, price);
                    price
                })
        }
    }
}
