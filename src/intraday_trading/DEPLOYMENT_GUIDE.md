# Production Deployment Guide

## 🚀 Quick Deploy Options

### Option 1: Railway.app (Recommended - Easiest)

1. **Install Railway CLI:**
   ```bash
   npm i -g @railway/cli
   railway login
   ```

2. **Deploy:**
   ```bash
   railway init
   railway up
   ```

3. **Railway auto-detects:**
   - FastAPI backend
   - Builds frontend automatically
   - Provides HTTPS URL automatically

**Free tier includes:** 500 hours/month, $5 credit

---

### Option 2: Render.com (Free Tier)

1. **Create account:** https://render.com
2. **New Web Service:**
   - Connect GitHub repo
   - Build command: `cd frontend && npm install && npm run build`
   - Start command: `cd .. && source .venv/bin/activate && python -m uvicorn alerts.optimized_main:app --host 0.0.0.0 --port $PORT`
   - Environment: Python 3
3. **Add Environment Variables:**
   - `ENVIRONMENT=production`
   - `FRONTEND_URL=https://your-service.onrender.com`
   - Redis connection string (if using external Redis)

**Free tier:** Sleeps after 15min inactivity, but free forever

---

### Option 3: Fly.io (Free Tier)

1. **Install Fly CLI:**
   ```bash
   curl -L https://fly.io/install.sh | sh
   flyctl auth login
   ```

2. **Create app:**
   ```bash
   flyctl launch
   ```

3. **Deploy:**
   ```bash
   flyctl deploy
   ```

**Free tier:** 3 shared VMs, 160GB outbound data

---

### Option 4: Local with Cloudflare Tunnel (Free, Permanent)

1. **Install cloudflared:**
   ```bash
   brew install cloudflare/cloudflare/cloudflared
   ```

2. **Start tunnel:**
   ```bash
   cloudflared tunnel --url http://localhost:5000
   ```

3. **Get permanent domain:**
   - Sign up at https://dash.cloudflare.com
   - Create tunnel with permanent domain
   - Configure DNS

**Free tier:** Unlimited tunnels, permanent domains

---

## 📦 Build Frontend for Production

```bash
cd frontend
npm run build
# Creates frontend/dist/ folder
```

## 🔧 Production Configuration

### Environment Variables

Create `.env` or set in deployment platform:

```bash
ENVIRONMENT=production
FRONTEND_URL=https://your-domain.com
ALLOW_ALL_ORIGINS=false  # Set to true only for development
REDIS_HOST=your-redis-host
REDIS_PORT=6379
```

### Update CORS Origins

In `alerts/optimized_main.py`, update:
```python
allowed_origins = [
    "https://yourdomain.com",
    "https://www.yourdomain.com",
]
```

### Run Production Server

```bash
# Build frontend first
cd frontend && npm run build && cd ..

# Run with production settings
source .venv/bin/activate
export ENVIRONMENT=production
python -m uvicorn alerts.optimized_main:app --host 0.0.0.0 --port 5000
```

Or with Gunicorn (better for production):
```bash
pip install gunicorn
gunicorn -w 4 -k uvicorn.workers.UvicornWorker alerts.optimized_main:app --bind 0.0.0.0:5000
```

---

## 🌐 Quick Start: Cloudflare Tunnel (Recommended for Local)

This gives you a permanent HTTPS URL that works from anywhere:

```bash
# 1. Install cloudflared
brew install cloudflare/cloudflare/cloudflared

# 2. Start tunnel (runs in background)
cloudflared tunnel --url http://localhost:5000

# 3. Copy the HTTPS URL (e.g., https://abc123.trycloudflare.com)
# 4. Update frontend/.env.local:
#    VITE_API_URL=https://abc123.trycloudflare.com/api
# 5. Rebuild frontend:
cd frontend && npm run build

# 6. Restart backend with production mode:
export ENVIRONMENT=production
python -m uvicorn alerts.optimized_main:app --host 0.0.0.0 --port 5000
```

Now your dashboard is accessible at: `https://abc123.trycloudflare.com`

---

## 📝 Notes

- **Frontend build:** The React app is built to `frontend/dist/` and served by FastAPI
- **API endpoints:** All `/api/*` routes work as before
- **WebSocket:** WebSocket connections work over HTTPS
- **Redis:** Make sure Redis is accessible from your deployment platform

