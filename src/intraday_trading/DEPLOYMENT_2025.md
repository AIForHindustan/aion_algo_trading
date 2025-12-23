# 🚀 2025 Deployment Guide - Modern Options

## Top Recommendations for FastAPI + React Dashboard

### 1. **Vercel** (⭐ Best for React + API Routes)

**Why 2025:**
- Zero-config deployment
- Automatic HTTPS
- Edge functions for FastAPI
- Free tier: Unlimited
- Auto-deploys from GitHub

**Setup:**
```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
cd /Users/lokeshgupta/Projects/aion_algo_trading/intraday_trading
vercel

# For production
vercel --prod
```

**Configuration:**
- Frontend auto-detected (React)
- API routes: Create `api/` folder or use serverless functions
- WebSocket: Use Vercel's Edge Network or upgrade to Pro

**Cost:** Free forever for hobby projects

---

### 2. **Cloudflare Workers + Pages** (⭐ Best for Global Edge)

**Why 2025:**
- Edge computing (fastest worldwide)
- Free tier: 100,000 requests/day
- Global CDN included
- Workers for API, Pages for React

**Setup:**
```bash
# Install Wrangler CLI
npm i -g wrangler

# Login
wrangler login

# Deploy frontend (Pages)
cd frontend
wrangler pages deploy dist

# Deploy backend (Workers)
# Create wrangler.toml for FastAPI adapter
```

**Cost:** Free tier generous, $5/month for more

---

### 3. **Coolify** (⭐ Best Self-Hosted PaaS)

**Why 2025:**
- Modern alternative to Heroku
- Self-hosted = full control
- Docker-based
- One-click deployments
- Free (just need a VPS)

**Setup:**
```bash
# Install on VPS (Ubuntu/Debian)
curl -fsSL https://cdn.coollabs.io/coolify/install.sh | bash

# Deploy via web UI or:
coolify deploy
```

**Requirements:** VPS (DigitalOcean, Hetzner, etc.) - $5-10/month

---

### 4. **Fly.io** (⭐ Best for Docker)

**Why 2025:**
- Modern Docker platform
- Global edge deployment
- Free tier: 3 shared VMs, 160GB outbound
- Great for FastAPI

**Setup:**
```bash
# Install Fly CLI
curl -L https://fly.io/install.sh | sh

# Login
flyctl auth login

# Create app
flyctl launch

# Deploy
flyctl deploy
```

**Cost:** Free tier available, pay-as-you-go

---

### 5. **Render** (Still Good in 2025)

**Why 2025:**
- Free tier still exists
- Auto-deploys from GitHub
- Simple setup
- Sleeps after 15min (free tier)

**Setup:**
1. Connect GitHub repo at render.com
2. New Web Service
3. Build: `cd frontend && npm install && npm run build`
4. Start: `python -m uvicorn alerts.optimized_main:app --host 0.0.0.0 --port $PORT`
5. Set `ENVIRONMENT=production`

**Cost:** Free (with sleep), $7/month for always-on

---

### 6. **CapRover** (Self-Hosted Alternative)

**Why 2025:**
- One-click Docker deployments
- Modern PaaS on your own server
- Free (just need VPS)
- Similar to Heroku but self-hosted

**Setup:**
```bash
# Install on VPS
docker run -p 80:80 -p 443:443 -p 3000:3000 -v /var/run/docker.sock:/var/run/docker.sock caprover/caprover

# Deploy via web UI
```

**Requirements:** VPS - $5-10/month

---

### 7. **Railway** (Updated 2025)

**Why 2025:**
- Auto-detects FastAPI
- Simple deployment
- Good developer experience

**Setup:**
```bash
npm i -g @railway/cli
railway login
railway init
railway up
```

**Cost:** $5/month minimum (free tier removed in 2024)

---

### 8. **DigitalOcean App Platform**

**Why 2025:**
- Simple git-based deploy
- Auto-scaling
- $5 credit/month (effectively free for small apps)

**Setup:**
1. Connect GitHub at cloud.digitalocean.com
2. Create App
3. Auto-detects FastAPI + React
4. Deploy

**Cost:** $5/month minimum (free credit)

---

## 🎯 Quick Comparison (2025)

| Option | Cost | Setup Time | Best For |
|--------|------|------------|----------|
| **Vercel** | Free | 5 min | React + API |
| **Cloudflare** | Free | 10 min | Global edge |
| **Coolify** | VPS cost | 15 min | Self-hosted |
| **Fly.io** | Free tier | 10 min | Docker apps |
| **Render** | Free* | 5 min | Simple deploy |
| **CapRover** | VPS cost | 20 min | Self-hosted |
| **Railway** | $5/mo | 5 min | Easy deploy |
| **DigitalOcean** | $5/mo | 10 min | Auto-scaling |

*Free tier sleeps after inactivity

---

## 🚀 Recommended: Vercel (Fastest Setup)

**Why Vercel in 2025:**
- Built specifically for React
- Zero configuration
- Automatic deployments
- Edge functions for API
- Free forever

**Quick Start:**
```bash
npm i -g vercel
cd /Users/lokeshgupta/Projects/aion_algo_trading/intraday_trading
vercel
```

That's it! You get a public URL instantly.

---

## 🔧 Alternative: Coolify (If You Want Control)

**Why Coolify:**
- Modern self-hosted PaaS
- Full control over infrastructure
- No vendor lock-in
- Free (just VPS cost)

**Setup:**
1. Get VPS (Hetzner $5/month or DigitalOcean $6/month)
2. Install Coolify: `curl -fsSL https://cdn.coollabs.io/coolify/install.sh | bash`
3. Deploy via web UI
4. Get permanent domain

---

## 📝 Next Steps

1. **Choose an option** based on your needs
2. **Deploy** using the commands above
3. **Update CORS** in `alerts/optimized_main.py` with your public URL
4. **Set environment** variables in deployment platform
5. **Test** the public URL

All options are production-ready and modern for 2025! 🎉

