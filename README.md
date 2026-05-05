# Moataz Alazzeh — Sales Performance Dashboard
NESTU Jordan | Powered by Odoo 17 | Auto-refreshes daily at 7:00am Amman time

## Setup
1. Add GitHub Secrets: `ODOO_USERNAME`, `ODOO_API_KEY`
2. Enable GitHub Pages: Settings → Pages → Source: Deploy from branch → main → / (root)
3. Dashboard URL: https://nestu-ltd.github.io/moataz-dashboard/dashboard.html

## Manual Refresh
GitHub → Actions → Refresh Moataz Dashboard → Run workflow

## Local Refresh
```
set ODOO_USERNAME=ghassan@nestu.health
set ODOO_API_KEY=your_key
python moataz_dashboard.py
```
