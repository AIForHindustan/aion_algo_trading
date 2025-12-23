# Backend API Endpoints

All endpoints are under `/api/` prefix except WebSocket and root routes.

## Authentication Endpoints
- `POST /api/auth/login` - Login with username/password (Form data)
- `POST /api/auth/register` - Register new user (requires auth)
- `POST /api/auth/create-user` - Create user (admin only)
- `POST /api/auth/refresh` - Refresh access token
- `POST /api/auth/2fa/setup` - Setup 2FA
- `POST /api/auth/2fa/verify` - Verify 2FA code
- `POST /api/auth/2fa/disable` - Disable 2FA

## Alerts Endpoints
- `GET /api/alerts` - Get paginated alerts (DB 1: `alerts:stream`)
- `GET /api/alerts/{alert_id}` - Get single alert (DB 0 + DB 1)
- `GET /api/alerts/stats/summary` - Alert statistics (DB 1: `alerts:stream`)

## Market Data Endpoints
- `GET /api/market/indices` - Market indices (DB 2: `ohlc_latest`, DB 1: `index:NSE:*`)
- `GET /api/charts/{symbol}` - OHLC chart data (DB 2: `ohlc_daily`, `ohlc_latest`)
- `GET /api/indicators/{symbol}` - Technical indicators (DB 5: `indicators:{symbol}:*`)
- `GET /api/greeks/{symbol}` - Options Greeks (DB 5: `indicators:{symbol}:greeks`)
- `GET /api/volume-profile/{symbol}` - Volume profile (DB 2: `volume_profile:poc:*`)

## News Endpoints
- `GET /api/news/{symbol}` - News by symbol (DB 1: `news:latest:{symbol}`)
- `GET /api/news/market/latest` - Latest market news (file system)

## Validation Endpoints
- `GET /api/validation/{alert_id}` - Validation results (DB 0: `forward_validation:alert:*`)
- `GET /api/validation/stats` - Validation stats (DB 0: `alert_performance:stats`)

## Instruments Endpoints
- `GET /api/instruments` - Get instruments list
- `GET /api/instruments/{asset_class}` - Get instruments by asset class
- `GET /api/instruments/search/{query}` - Search instruments
- `GET /api/instruments/metadata` - Instrument metadata
- `GET /api/options-chain/{underlying}` - Options chain

## Brokerage Endpoints
- `GET /api/brokerage/auth-url` - Get brokerage auth URL
- `POST /api/brokerage/verify-connection` - Verify brokerage connection
- `GET /api/brokerage/connections` - Get brokerage connections
- `POST /api/brokerage/disconnect` - Disconnect brokerage

## WebSocket Endpoints
- `WS /ws/professional/{symbol}` - Real-time chart updates (DB 2: `ohlc_latest`, DB 1: `index:NSE:*`)

## Root Endpoints
- `GET /` - Root endpoint
- `GET /professional` - Professional dashboard HTML
- `GET /alerts/stream` - Alerts stream endpoint

