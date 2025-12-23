#!/bin/bash
# Production deployment script

echo "🚀 Building frontend for production..."
cd frontend
npm run build
cd ..

echo "✅ Frontend built to frontend/dist/"

echo ""
echo "📦 Production deployment options:"
echo ""
echo "1. Local with Cloudflare Tunnel (recommended):"
echo "   cloudflared tunnel --url http://localhost:5000"
echo ""
echo "2. Railway.app:"
echo "   railway up"
echo ""
echo "3. Render.com:"
echo "   Connect GitHub repo at render.com"
echo ""
echo "4. Run locally in production mode:"
echo "   export ENVIRONMENT=production"
echo "   source .venv/bin/activate"
echo "   python -m uvicorn alerts.optimized_main:app --host 0.0.0.0 --port 5000"
echo ""
