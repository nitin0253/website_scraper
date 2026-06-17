# Inventory Scraper UI

Web interface for the multi-rooftop dealer inventory scraper.  
Upload your CSV → watch live progress → download all CSVs.

---

## Project structure

```
scraper-ui/
├── api/
│   └── index.py          ← FastAPI backend (runs the scraper)
├── public/
│   └── index.html        ← Frontend UI
├── requirements.txt
├── vercel.json
└── README.md
```

---

## Option 1 — Run locally

```bash
# Install dependencies
pip install -r requirements.txt

# Start the server
uvicorn api.index:app --reload --port 8000
```

Then open `http://localhost:8000` in your browser.

> **Note:** When running locally, edit the `const API` line in `public/index.html`:
> ```js
> const API = 'http://localhost:8000';
> ```
> Change it back to `''` before deploying.

---

## Option 2 — Deploy to Vercel (free)

1. Push this folder to a GitHub repo
2. Go to [vercel.com](https://vercel.com) → New Project → Import your repo
3. Vercel auto-detects the config from `vercel.json`
4. Click Deploy — done

> **Important:** Vercel's free tier has a **10-second function timeout**.  
> For large rooftop lists (20+ rooftops), the scrape will exceed this.  
> Use the **Railway** option below for production use.

---

## Option 3 — Deploy to Railway (recommended for production)

Railway gives you a persistent server with no timeout limit.

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login and deploy
railway login
railway init
railway up
```

Set environment variable if needed:
```
PORT=8000
```

Start command: `uvicorn api.index:app --host 0.0.0.0 --port $PORT`

---

## Option 4 — GitHub Pages (frontend only)

GitHub Pages only serves static files — no Python backend.  
You can host just the UI and point it at a separately hosted backend:

1. Edit `const API` in `public/index.html` to your backend URL
2. Push `public/index.html` to a `gh-pages` branch
3. Enable GitHub Pages in repo Settings → Pages

---

## Input CSV format

Required columns (column names are case-sensitive, extra columns ignored):

| Column | Description |
|---|---|
| `Enterprise_Name` | Enterprise / dealer group name |
| `Rooftop_Name` | Individual rooftop name |
| `New_url` | URL of new inventory page |
| `used_url` | URL of used inventory page |
| `Website Link` | (optional) base domain |

---

## Output files

Each run produces files in a temp directory:

| File | Contents |
|---|---|
| `{rooftop_slug}_new.csv` | New inventory VINs + images |
| `{rooftop_slug}_used.csv` | Used inventory VINs + images |
| `run_log.csv` | Full run summary with errors |

Each inventory CSV has columns:  
`enterprise_name, rooftop_name, condition, vin, first_image_url`
