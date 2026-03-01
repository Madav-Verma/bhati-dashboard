# Bhati March 2026 — Dashboard Setup Guide
### Step by step, zero GitHub knowledge needed.

---

## What You'll Have After This
- A Python script running automatically every 10 mins on GitHub's servers
- A live public dashboard URL you can share with anyone
- Minimal Firestore reads (incremental, only new scans each time)

---

## STEP 1 — Create a GitHub Account

1. Go to **https://github.com**
2. Click **Sign up**
3. Enter your email, create a password, choose a username
4. Verify your email

---

## STEP 2 — Create a New Repository

1. After logging in, click the **+** button (top right) → **New repository**
2. Name it: `bhati-dashboard` (no spaces)
3. Set it to **Public** ← important for GitHub Pages
4. Check ✅ **Add a README file**
5. Click **Create repository**

---

## STEP 3 — Upload Your Files

You need to upload these 4 files to the repo:
```
fetch_data.py
index.html
requirements.txt
.github/workflows/fetch.yml     ← this one needs a special step
```

### Upload fetch_data.py, index.html, requirements.txt:
1. In your repo, click **Add file** → **Upload files**
2. Drag and drop these 3 files
3. Scroll down, click **Commit changes**

### Upload the workflow file (special step):
The `.github/workflows/` folder needs to be created first.

1. Click **Add file** → **Create new file**
2. In the filename box type exactly: `.github/workflows/fetch.yml`
   (GitHub will auto-create the folders as you type the slashes)
3. Open the `fetch.yml` file from your computer, copy ALL its contents
4. Paste into the text editor on GitHub
5. Click **Commit changes**

---

## STEP 4 — Add Your Firebase Secret

This is the most important step. Your `service_account.json` must be kept private.

1. Open your `service_account.json` file (the one on your Desktop)
2. Select ALL its contents (Ctrl+A) and Copy (Ctrl+C)
3. In your GitHub repo, go to:
   **Settings** → **Secrets and variables** → **Actions** → **New repository secret**
4. Name: `FIREBASE_SERVICE_ACCOUNT`
5. Value: Paste the entire JSON content here
6. Click **Add secret**

---

## STEP 5 — Enable GitHub Pages

1. In your repo go to **Settings** → **Pages** (left sidebar)
2. Under **Source**, select **Deploy from a branch**
3. Branch: **main**, Folder: **/ (root)**
4. Click **Save**

After ~2 minutes, your dashboard will be live at:
```
https://YOUR-USERNAME.github.io/bhati-dashboard/
```

---

## STEP 6 — Run It For The First Time

1. In your repo go to **Actions** tab
2. Click **Fetch Bhati Attendance Data** (left sidebar)
3. Click **Run workflow** → **Run workflow** (green button)
4. Wait ~1 minute — you'll see a green ✅ when done
5. Visit your dashboard URL — data should be live!

After this first manual run, it will run **automatically every 10 minutes** on its own.

---

## Checking If It's Working

- Go to **Actions** tab in your repo
- You'll see a list of runs — green ✅ = success, red ❌ = something failed
- Click any run to see the logs and output

---

## Common Issues

**❌ Workflow failing?**
→ Double check your `FIREBASE_SERVICE_ACCOUNT` secret — make sure you copied the FULL JSON

**❌ Dashboard shows "Could not load data.json"?**
→ The workflow hasn't run yet, or GitHub Pages isn't enabled. Wait 2-3 mins and refresh.

**❌ No data showing?**
→ Check the Actions log — it will tell you exactly what went wrong

---

## Your Public Dashboard URL
```
https://YOUR-USERNAME.github.io/bhati-dashboard/
```
Replace `YOUR-USERNAME` with your actual GitHub username.
Share this with anyone — it updates every 10 minutes automatically.
