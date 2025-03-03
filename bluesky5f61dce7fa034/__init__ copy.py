import aiohttp
import asyncio
import random
import logging
from datetime import datetime, timedelta
import hashlib
from typing import AsyncGenerator, Any, Dict, List
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    ExternalId,
    Url,
    Domain,
)
logging.basicConfig(level=logging.INFO)

# Constants
SPECIAL_KEYWORDS_LIST = [    
    "the",
    "the",
    "lord",
    "ords",
    "brc20",
    "paris2024",
    "paris2024",
    "olympic",
    "olympic",
    "Acura",
    "Alfa Romeo",
    "Aston Martin",
    "Audi",
    "Bentley",
    "BMW",
    "Buick",
    "Cadillac",
    "Chevrolet",
    "Chrysler",
    "Dodge",
    "Ferrari",
    "Fiat",
    "Ford",
    "Genesis",
    "GMC",
    "Honda",
    "Hyundai",
    "Infiniti",
    "Jaguar",
    "Jeep",
    "Kia",
    "Lamborghini",
    "Land Rover",
    "Lexus",
    "Lincoln",
    "Lotus",
    "Maserati",
    "Mazda",
    "Taiko",
    "Taiko labs",
    "McLaren",
    "Mercedes-Benz",
    "MINI",
    "Mitsubishi",
    "Nissan",
    "Porsche",
    "Ram",
    "Renault",
    "Rolls-Royce",
    "Subaru",
    "Tesla",
    "Toyota",
    "Volkswagen",
    "Volvo",    
    "BlackRock",
    "Vanguard",
    "State Street",
    "advisors",
    "Fidelity",
    "Fidelity Investments",
    "Asset Management",
    "Asset",
    "digital asset",
    "NASDAQ Composite",
    "Dow Jones Industrial Average",
    "Gold",
    "Silver",
    "Brent Crude",
    "WTI Crude",
    "EUR",
    "US",
    "YEN"
    "UBS",
    "PIMCO",
    "schroders",
    "aberdeen",    
    "louis vuitton",
    "moet Chandon",
    "hennessy",
    "dior",
    "fendi",
    "givenchy",
    "celine",
    "tag heuer",
    "bvlgari",
    "dom perignon",
    "hublot",
    "Zenith",    
    "meme", 
    "coin", 
    "memecoin", 
    "pepe", 
    "doge", 
    "shib",
    "floki",
    "dogtoken",
    "trump token",
    "barron token",    
    "DOGE",
    "SHIB",
    "PEPE",
    "BONK",
    "WIF",
    "FLOKI",
    "MEME",    
    "DOGE",
    "SHIB",
    "PEPE",
    "BONK",
    "WIF",
    "FLOKI",
    "MEME",
    "DOGE",
    "SHIB",
    "PEPE",
    "BONK",
    "WIF",
    "FLOKI",
    "MEME",
    "TRUMP",
    "BabyDoge",
    "ERC20",
    "BONE",
    "COQ",
    "WEN",
    "BITCOIN",
    "ELON",
    "SNEK",
    "MYRO",
    "PORK",
    "TOSHI",
    "SMOG",
    "LADYS",
    "AIDOGE",
    "TURBO",
    "TOKEN",
    "SAMO",
    "KISHU",
    "TSUKA",
    "LEASH",
    "QUACK",
    "VOLT",
    "PEPE2.0",
    "JESUS",
    "MONA",
    "DC",
    "WSM",
    "PIT",
    "QOM",
    "PONKE",
    "SMURFCAT",
    "AKITA",
    "VINU",
    "ANALOS",
    "BAD",
    "CUMMIES",
    "HONK",
    "HOGE",
    "$MONG",
    "SHI",
    "BAN",
    "RAIN",
    "TAMA",
    "PAW",
    "SPX",
    "HOSKY",
    "BOZO",
    "DOBO",
    "PIKA",
    "CCC",
    "REKT",
    "WOOF",
    "MINU",
    "WOW",
    "PUSSY",
    "KEKE",
    "DOGGY",
    "KINGSHIB",
    "CHEEMS",
    "SMI",
    "OGGY",
    "DINGO",
    "DONS",
    "GRLC",
    "AIBB",
    "CATMAN",
    "XRP",
    "CAT",
    "数字資産",  # Digital Asset (Japanese)
    "仮想",  # Virtual (Japanese)
    "仮想通貨",  # Virtual Currency (Japanese)
    "自動化",  # Automation (Japanese)
    "アルゴリズム",  # Algorithm (Japanese)
    "コード",  # Code (Japanese)
    "機械学習",  # Machine Learning (Japanese)
    "ブロックチェーン",  # Blockchain (Japanese)
    "サイバーセキュリティ",  # Cybersecurity (Japanese)
    "人工",  # Artificial (Japanese)
    "合成",  # Synthetic (Japanese)
    "主要",  # Major (Japanese)
    "IoT",
    "クラウド",  # Cloud (Japanese)
    "ソフトウェア",  # Software (Japanese)
    "API",
    "暗号化",  # Encryption (Japanese)
    "量子",  # Quantum (Japanese)
    "ニューラルネットワーク",  # Neural Network (Japanese)
    "オープンソース",  # Open Source (Japanese)
    "ロボティクス",  # Robotics (Japanese)
    "デブオプス",  # DevOps (Japanese)
    "5G",
    "仮想現実",  # Virtual Reality (Japanese)
    "拡張現実",  # Augmented Reality (Japanese)
    "バイオインフォマティクス",  # Bioinformatics (Japanese)
    "ビッグデータ",  # Big Data (Japanese)
    "大統領",  # President (Japanese)
    "行政",  # Administration (Japanese)
    "Binance",
    "Bitcoin ETF",
    "政治",  # Politics (Japanese)
    "政治的",  # Political (Japanese)
    "ダイアグラム",  # Diagram (Japanese)
    "$algo",
    "$algo",
    "%23CAC",
    "%23G20",
    "%23IPO",
    "%23NASDAQ",
    "%23NYSE",
    "%23OilPrice",
    "%23SP500",
    "%23USD",
    "%23airdrop",
    "%23altcoin",
    "%23bonds",
    "%23price",
    "AI",
    "AI",
    "AI",
    "AI",
    "AUDNZD",
    "Alphabet%20(GOOG)",
    "Apple",
    "Aprendizaje Automático",
    "BNB",
    "Berkshire",
    "Biden administration",
    "Binance",
    "Bitcoin%20ETF",
    "Black%20Rock",
    "BlackRock",
    "BlackRock",
    "Branche",
    "Brazil",
    "CAC40",
    "COIN",
    "Canada",
    "China",
    "Coinbase",
    "Congress",
    "Crypto",
    "Crypto",
    "Crypto",
    "Cryptocurrencies",
    "Cryptos",
    "DeFi",
    "Diagramm",
    "Dios mío",
    "DowJones",
    "ETF",
    "ETFs",
    "EU",
    "EU",
    "EURUSD",
    "Elon",
    "Elon",
    "Elon",
    "Elon%20musk",
    "Europe",
    "European%20union%20(EU)",
    "FB%20stock",
    "FTSE",
    "Firma",
    "France",
    "GDP",
    "GPU",
    "GameFi",
    "Gensler",
    "Germany",
    "Gerücht",
    "Geschäft",
    "Gesundheit",
    "Gewinn",
    "Gewinn",
    "Heilung",
    "IA",
    "IA",
    "IPO",
    "Israel",
    "Israel",
    "Israel",
    "Juego",
    "KI",
    "Konflikt",
    "Kraken",
    "LGBTQ rights",
    "LVMH",
    "Land",
    "Luxus",
    "Marke",
    "Maschinelles Lernen",
    "Mexico",
    "NFLX",
    "NFT",
    "NFT",
    "NFTs",
    "NYSE",
    "Nachrichten",
    "Nasdaq%20100",
    "Oh Dios mío",
    "Openfabric",
    "Openfabric AI",
    "Openfabric",
    "OFN",
    "PLTR",
    "Palestine",
    "Palestine",
    "Palestine",
    "País",
    "Politik",
    "Produkt",
    "Roe v. Wade",
    "Silicon Valley",
    "Spiel",
    "Spot%20ETF",
    "Start-up",
    "Streaming",
    "Supreme Court",
    "Technologie",
    "Tesla",
    "UE",
    "UE",
    "USA",
    "USDEUR",
    "United%20states",
    "Unterhaltung",
    "Verlust",
    "Virus",
    "Vorhersage",
    "WallStreet",
    "WarrenBuffett",
    "Warren Buffett",
    "Web3",
    "X.com",
    "XAUUSD",
    "Xitter",
    "abortion",
    "achetez",
    "actualité",
    "airdrop",
    "airdrops",
    "alert",
    "algorand",
    "algorand",
    "algorand",
    "amazon",
    "analytics",
    "announcement",
    "apprentissage",
    "artificial intelligence",
    "artificial intelligence",
    "asset",
    "asset%20management",
    "attack",
    "attack",
    "attack",
    "attentat",
    "authocraty",
    "balance sheet",
    "bank",
    "bear",
    "bearish",
    "bears",
    "beliebt",
    "bezos",
    "biden",
    "biden",
    "biden",
    "biden",
    "data",
    "develop",
    "virtual",
    "automation",
    "algorithm",
    "code",
    "machine learning",
    "blockchain",
    "cybersecurity",
    "artificial",
    "synth",
    "synthetic",
    "major",
    "IoT",
    "cloud",
    "software",
    "API",
    "encryption",
    "quantum",
    "neural",
    "open source",
    "robotics",
    "devop",
    "5G",
    "virtual reality",
    "augmented reality",
    "bioinformatics",
    "big data",
    "billion",
    "bitcoin",
    "bizness",
    "blockchain",
    "bond",
    "breaking news",
    "breaking%20news",
    "btc",
    "btc",
    "btc",
    "btc",
    "btc",
    "btc",
    "btc",
    "btc",
    "budget",
    "bull",
    "bullish",
    "bulls",
    "business",
    "businesses",
    "buy support",
    "cardano",
    "cash flow",
    "cbdc",
    "choquant",
    "climate change action",
    "climate change",
    "climate tech startups",
    "communist",
    "companies",
    "company",
    "compound interest",
    "compra ahora"
    "compra",
    "conflict",
    "conflict",
    "conflicto",
    "conflit",
    "congress",
    "conservatives",
    "corporate",
    "corporation",
    "credit",
    "crime",
    "crisis",
    "crude%20oil",
    "crypto",
    "crypto",
    "crypto",
    "crypto",
    "crypto",
    "cryptocurrency",
    "cryptocurrency",
    "cura",
    "currencies",
    "currency",
    "currency",
    "database",
    "debit",
    "debt",
    "debt",
    "decentralized finance",
    "decentralized",
    "decline",
    "deep learning",
    "defi",
    "democracy",
    "diffusion",
    "digital",
    "divertissement",
    "dividend",
    "doge",
    "dogecoin",
    "démarrage",
    "e-commerce",
    "economy",
    "economy",
    "education startups",
    "education",
    "elections",
    "elisee",
    "embargo",
    "embassy",
    "empresa",
    "entreprise",
    "entretenimiento",
    "equity",
    "erc20",
    "eth",
    "eth",
    "eth",
    "eth",
    "eth",
    "ethereum",
    "exchange rate",
    "expense",
    "extremism",
    "fair%20launch",
    "fascist",
    "finance",
    "finance",
    "financial advisor",
    "financial planning",
    "financing",
    "fintech",
    "fintech",
    "fintech",
    "fiscal policy",
    "fixed income",
    "foreign aid",
    "foreign exchange",
    "foreign policy",
    "forex",
    "forex",
    "founder CEO",
    "founders",
    "fusion",
    "gagner",
    "gain",
    "ganancia",
    "ganar",
    "gas",
    "gaza",
    "gaza",
    "gaza",
    "government",
    "governments",
    "graphique",
    "gross domestic product",
    "growth",
    "gráfico",
    "gun control",
    "gun violence prevention",
    "hamas",
    "hamas",
    "hamas",
    "hamas",
    "healthcare startups",
    "healthcare",
    "helion",
    "hft trading",
    "holdings",
    "hostage",
    "hostage",
    "immigration reform",
    "immigration",
    "impactante",
    "impactante",
    "impeachment",
    "income",
    "increíble",
    "increíble",
    "incroyable",
    "industria",
    "industrie",
    "inflation",
    "inflation",
    "infrastructure",
    "insider trading",
    "insider",
    "insurance",
    "intraday",
    "investing",
    "investment",
    "investor",
    "investors",
    "jerusalem",
    "jeu",
    "kaufen",
    "kremlin",
    "legal",
    "legal%20tender",
    "liability",
    "libertarian",
    "liquidity",
    "loan",
    "long",
    "lujo",
    "luxe",
    "machine learning",
    "macron",
    "macron",
    "macron",
    "en marche",
    "parti",
    "marca",
    "margin",
    "mark%20zuckerberg",
    "market capitalization",
    "market maker",
    "market",
    "markets",
    "marque",
    "mein Gott",
    "middle east",
    "middle east",
    "middle east",
    "million",
    "mint",
    "missile",
    "missile",
    "missile",
    "mon Dieu",
    "monero",
    "money",
    "mortgage",
    "moscow",
    "mutual fund",
    "nasdaq",
    "national security",
    "national%20emergency",
    "national%20security",
    "natural%20gas",
    "negocios",
    "net income",
    "net worth",
    "new project",
    "new startup",
    "news",
    "newsfeed",
    "newsflash",
    "nft",
    "nft%20latform",
    "nftcommunity",
    "nfts",
    "noticias",
    "nuclear",
    "official",
    "oil",
    "parliament",
    "pays",
    "perder",
    "perdido",
    "perdre",
    "perdu",
    "plummet",
    "police",
    "politician",
    "politicians",
    "politique",
    "polkadot",
    "polygon",
    "política",
    "populaire",
    "popular",
    "populism",
    "portfolio",
    "predicción",
    "press",
    "price-to-earnings ratio",
    "producto",
    "produit",
    "profit",
    "promising company",
    "protocols",
    "prédiction",
    "putin",
    "putin",
    "putin",
    "putin",
    "poutine",
    "poutine",
    "poutine",
    "poutine",
    "vladimir putin",
    "vladimir putin",
    "vladimir putin",
    "xi jinping",
    "xi jinping",
    "xi jinping",
    "racial justice",
    "recession",
    "renault trucks",
    "renault trucks",
    "renault",
    "renault",
    "resistance sell",
    "retirement planning",
    "return on investment",
    "riots",
    "ripple",
    "risk",
    "robotics",
    "rumeur",
    "rumor",
    "russia",
    "s&p500",
    "sales",
    "salud",
    "sam20altman",
    "santé",
    "satoshi",
    "schockierend",
    "scraping",
    "securities",
    "security%20token",
    "self-driving cars",
    "senate",
    "senator",
    "senators",
    "shardeum",
    "short",
    "silvio micali",
    "solana",
    "solana%20sol",
    "sp500",
    "space exploration",
    "space tech startups",
    "stablecoin",
    "startup",
    "startup",
    "stock market",
    "stock",
    "stocks",
    "streaming",
    "syria",
    "takeoff",
    "tax",
    "tech startups",
    "technologie",
    "technology",
    "tecnología",
    "token",
    "toyota",
    "trade",
    "trading",
    "trading",
    "traitement",
    "treasury bill",
    "trump",
    "trump",
    "trump",
    "trump",
    "vote",
    "vote",
    "vote",
    "election",
    "election",
    "election",
    "voter",
    "voter",
    "million",
    "club",
    "tech",
    "nvda",
    "machine",
    "generative",
    "reinforcement",
    "official",
    "twitter",
    "ukraine",
    "unglaublich",
    "unicorns",
    "unicorns",
    "us%20president",
    "usdt",
    "usdt",
    "usdt",
    "usdt",
    "utility%20token",
    "venture capital",
    "venture capital",
    "venture capital",
    "verloren",
    "virus",
    "virus",
    "volvo group",
    "volvo trucks",
    "volvo",
    "voting rights",
    "wall street",
    "war in Ukraine",
    "war",
    "web3",
    "web3",
    "white house",
    "worldcoin",
    "xrp",
    "yield",
    "zero knowledge",
    "zksync",    
    "renewables",
    "energy",
    "infrastructure",
    "infrastructure investment",
    "FDI investment",
    "foreign investment",
    "foreign policy",
    "new policy",
    "new policies",
    "ГПУ",
    "ЕС",
    "ИИ",
    "Игра",
    "Илон",
    "Машинное обучение",
    "Страна",
    "бизнес",
    "бренд",
    "вирус",
    "график",
    "здоровье",
    "индустрия",
    "компания",
    "конфликт",
    "купи сейчас",
    "лечение",
    "невероятный",
    "новости",
    "о боже мой",
    "победа",
    "политика",
    "популярный",
    "поражение",
    "потеря",
    "потоковая передача",
    "прибыль",
    "прогноз",
    "продукт",
    "развлечение",
    "роскошь",
    "слух",
    "стартап",
    "технологии",
    "шокирующий",
    "أخبار",
    "أعمال",
    "إيلون",
    "اشتر الآن",
    "الاتحاد الأوروبي",
    "الذكاء الاصطناعي",
    "بث مباشر",
    "بلد",
    "ترفيه",
    "تعلم الآلة",
    "تكنولوجيا",
    "توقع",
    "خسارة",
    "رائع",
    "ربح",
    "رسم بياني",
    "سياسة",
    "شائعة",
    "شركة ناشئة",
    "شركة",
    "شهير",
    "صادم",
    "صحة",
    "صراع",
    "صناعة",
    "ضائع",
    "علاج",
    "علامة تجارية",
    "فخامة",
    "فوز",
    "فيروس",
    "لعبة",
    "منتج",
    "وحدة معالجة الرسومات",
    "يا إلهي",
    "ああ、神様",
    "イーロン",
    "ウイルス",
    "エンターテインメント",
    "ゲーム",
    "スタートアップ",
    "ストリーミング",
    "チャート",
    "テクノロジー",
    "ニュース",
    "ビジネス",
    "ブランド",
    "业务",
    "予測",
    "产品",
    "人工智能",
    "人気",
    "今買う",
    "令人难以置信",
    "令人震惊",
    "会社",
    "信じられない",
    "健康",
    "健康",
    "公司",
    "冲突",
    "初创企业",
    "利益",
    "勝利",
    "品牌",
    "哦，我的天啊",
    "噂",
    "国",
    "国家",
    "图表",
    "埃隆",
    "失われた",
    "失去",
    "娱乐",
    "技术",
    "收益",
    "政治",
    "政治",
    "敗北",
    "新闻",
    "机器学习",
    "機械学習",
    "欧盟",
    "治疗",
    "治療",
    "流媒体",
    "游戏",
    "热门",
    "産業",
    "病毒",
    "立刻购买",
    "紛争",
    "行业",
    "衝撃的",
    "製品",
    "谣言",
    "豪华",
    "赢",
    "输",
    "预测",
    "高級"
    ]

BASE_KEYWORDS = [
    'the', 'of', 'and', 'a', 'in', 'to', 'is', 'that', 'it', 'for', 'on', 'you', 'this', 'with', 'as', 'I', 'be', 'at', 'by', 'from', 'or', 'an', 'have', 'not', 'are', 'but', 'we', 'they', 'which', 'one', 'all', 'their', 'there', 'can', 'has', 'more', 'do', 'if', 'will', 'about', 'up', 'out', 'who', 'get', 'like', 'when', 'just', 'my', 'your', 'what',
    'el', 'de', 'y', 'a', 'en', 'que', 'es', 'la', 'lo', 'un', 'se', 'no', 'con', 'una', 'por', 'para', 'está', 'son', 'me', 'si', 'su', 'al', 'desde', 'como', 'todo', 'está',
    '的', '是', '了', '在', '有', '和', '我', '他', '这', '就', '不', '要', '会', '能', '也', '去', '说', '所以', '可以', '一个',
    'का', 'है', 'हों', 'पर', 'ने', 'से', 'कि', 'यह', 'तक', 'जो', 'और', 'एक', 'हिंदी', 'नहीं', 'आप', 'सब', 'तो', 'मुझे', 'इस', 'को',
    'في', 'من', 'إلى', 'على', 'مع', 'هو', 'هي', 'هذا', 'تلك', 'ون', 'كان', 'لك', 'عن', 'ما', 'ليس', 'كل', 'لكن', 'أي', 'ودي', 'أين',
    'র', 'এ', 'আমি', 'যা', 'তা', 'হয়', 'হবে', 'তুমি', 'কে', 'তার', 'এখন', 'এই', 'কিন্তু', 'মাঠ', 'কি', 'আপনি', 'বাহী', 'মনে', 'তাহলে', 'কেন', 'থাক',
    'o', 'a', 'e', 'de', 'do', 'da', 'que', 'não', 'em', 'para', 'como', 'com', 'um', 'uma', 'meu', 'sua', 'se', 'este', 'esse', 'isto',
    'в', 'и', 'не', 'на', 'что', 'как', 'что', 'он', 'она', 'это', 'но', 'с', 'из', 'по', 'к', 'то', 'да', 'был', 'который', 'кто',
    'の', 'に', 'は', 'を', 'です', 'ます', 'た', 'て', 'いる', 'い', 'この', 'それ', 'あ', '等', 'や', 'も', 'もし', 'いつ', 'よ', 'お',
    'der', 'die', 'das', 'und', 'in', 'zu', 'von', 'mit', 'ist', 'an', 'bei', 'ein', 'eine', 'nicht', 'als', 'auch', 'so', 'wie', 'was', 'oder',
    'le', 'la', 'à', 'de', 'et', 'un', 'une', 'dans', 'ce', 'que', 'il', 'elle', 'est', 's', 'des', 'pour', 'par', 'au', 'en', 'si',
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
]


# Define your list of proxies
PROXY_LIST = [
    "http://proxy-host-01:3128",
    "http://proxy-host-02:3128",
    "http://proxy-host-03:3128",
    "http://proxy-host-04:3128",
    "http://proxy-host-05:3128",
    # Add more proxies as needed
]

DEFAULT_OLDNESS_SECONDS = 3600
DEFAULT_MAXIMUM_ITEMS = 20
DEFAULT_MIN_POST_LENGTH = 10
DEFAULT_MAX_CONCURRENT_QUERIES = 20

async def fetch_posts(session: aiohttp.ClientSession, keyword: str, since: str, proxy: str) -> list:
    url = f"https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts?q={keyword}&since={since}"
    async with session.get(url, proxy=proxy) as response:
        if response.status == 200:
            data = await response.json()
            return data.get('posts', [])
        else:
            logging.error(f"Failed to fetch posts for keyword {keyword} using proxy {proxy}: {response.status}")
            return []

def calculate_since(max_oldness_seconds: int) -> str:
    since_time = datetime.utcnow() - timedelta(seconds=max_oldness_seconds)
    return since_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def convert_to_web_url(uri: str, user_handle: str) -> str:
    base_url = "https://bsky.app/profile"
    post_id = uri.split("/")[-1]
    web_url = f"{base_url}/{user_handle}/post/{post_id}"
    return web_url

def format_date_string(date_string: str) -> str:
    try:
        dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        try:
            dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            try:
                dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z")
            except ValueError:
                try:
                    dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S%z")
                except ValueError:
                    raise ValueError(f"Unsupported date format: {date_string}")

    formatted_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return formatted_timestamp

def read_parameters(parameters):
    if parameters and isinstance(parameters, dict):
        max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
    else:
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH

    return (
        max_oldness_seconds,
        maximum_items_to_collect,
        min_post_length
    )

async def query_single_keyword(keyword: str, since: str, proxy: str, max_items: int, min_post_length: int) -> List[Item]:
    items = []
    async with aiohttp.ClientSession() as session:
        posts = await fetch_posts(session, keyword, since, proxy)
        for post in posts:
            try:
                if len(items) >= max_items:
                    break

                datestr = format_date_string(post['record']["createdAt"])
                author_handle = post["author"]["handle"]

                sha1 = hashlib.sha1()
                sha1.update(author_handle.encode())
                author_sha1_hex = sha1.hexdigest()

                url_recomposed = convert_to_web_url(post["uri"], author_handle)
                full_content = post["record"]["text"] + " " + " ".join(
                    image.get("alt", "") for image in post.get("record", {}).get("embed", {}).get("images", [])
                )

                logging.info(f"[Bluesky] Found post: url: %s, date: %s, content: %s", url_recomposed, datestr, full_content)

                item_ = Item(
                    content=Content(str(full_content)),
                    author=Author(str(author_sha1_hex)),
                    created_at=CreatedAt(str(datestr)),
                    domain=Domain("bsky.app"),
                    external_id=ExternalId(post["uri"]),
                    url=Url(url_recomposed),
                )
                items.append(item_)

            except Exception as e:
                logging.exception(f"[Bluesky] Error processing post: {e}")

    return items

async def query(parameters: dict) -> AsyncGenerator[Dict[str, Any], None]:
    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    max_concurrent_queries = parameters.get("max_concurrent_queries", DEFAULT_MAX_CONCURRENT_QUERIES)

    since = calculate_since(max_oldness_seconds)
    yielded_items = 0

    tasks = []
    for i in range(max_concurrent_queries):
        if yielded_items >= maximum_items_to_collect:
            break

        if random.random() < 0.33 and SPECIAL_KEYWORDS_LIST:
            search_keyword = random.choice(SPECIAL_KEYWORDS_LIST)
        elif random.random() < 0.33 and BASE_KEYWORDS:
            search_keyword = random.choice(BASE_KEYWORDS)
        else:
            search_keyword = parameters.get("keyword", "default")

        proxy = random.choice(PROXY_LIST)
        task = query_single_keyword(search_keyword, since, proxy, maximum_items_to_collect, min_post_length)
        tasks.append(task)

    for task in asyncio.as_completed(tasks):
        results = await task
        for item in results:
            yield item
            yielded_items += 1
            if yielded_items >= maximum_items_to_collect:
                break
        if yielded_items >= maximum_items_to_collect:
            break

# Example usage:
# parameters = {
#     "max_oldness_seconds": 3600,
#     "maximum_items_to_collect": 10,
#     "min_post_length": 10,
#     "max_concurrent_queries": 3,
#     "keyword": "sample",
# }
# asyncio.run(query(parameters))
