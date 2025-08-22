# Data Pipeline Test Project



## 🔧 Setup Instructions

### 🐳 Docker

Make sure you have Docker installed and running on your machine.

To install it:

#### Mac

```
# 1. Install Colima - lightweight container runtime
brew install colima

# 2. (Optional) Install Docker CLI if you don’t have it
brew install docker

# 3. Start Colima with default settings
colima start
```

#### Windows

```
# 1. Install Docker Desktop for Windows
https://docs.docker.com/desktop/install/windows-install/

# 2. Start Docker Desktop
```

### 🐍 Python Setup (using `uv`)

```bash
uv venv
source .venv/bin/activate
uv pip install pandas sqlalchemy psycopg2-binary python-dotenv
```

Ensure you have a .env file in the project with the connection string

```
DB_URL=postgresql://postgres:password@127.0.0.1:55432/test_db
```

### Optional for VSCode

To view a list of `TODO` tasks in the project use the VSCode Todo-tree extension

- Open VS Code → Extensions panel (Cmd+Shift+X or Ctrl+Shift+X)
- Search for Todo Tree → Click Install.

OR

- Open VS Code → Press Cmd+Shift+P (Mac) or Ctrl+Shift+P (Windows/Linux).
- Type `Shell Command: Install 'code' command in PATH` → press Enter.

  - This allows you to run VS Code commands from the terminal.

- Install the extension from the terminal → `code --install-extension Gruntfuggly.todo-tree`

Tasks to complete are displayed in the tool-tree sidebar in VSCode

---

### 🐘 Start PostgreSQL via Docker

```bash
make db-start
```

> If you've run this project before and the database volume persists, run:

```bash
make db-stop
docker volume rm data-pipeline-test_pgdata
make db-start
```

## 🚀 Run the Pipeline

```bash
make run
```

You should see output like:

```
✅ Database connection successful.
Transforming data...
```

## 🧹 Other Make Commands

| Command         | What it does                       |
| --------------- | ---------------------------------- |
| `make setup`    | Create virtualenv and install deps |
| `make run`      | Run the ETL pipeline               |
| `make db-start` | Start Postgres via Docker          |
| `make db-stop`  | Stop the Docker container          |
| `make clean`    | Remove venv, pycache, etc.         |

---

## The Promblem

Our system tracks the `credits` a user has used when they perform certain actions in the platform.
Unfortunatley somone gave our CTO access to the logs and the 2024 Q4 logs have "disappeared" 🤨 - and a log of actions has been cobbled
together from various other places - leaving us with a messy cvs of user actions to sort out before we save them to our database.

**Our customer sucess team really want to know who is using the system, for what and when.**

**Our finance team are desperate for a total credit balance of each organisation so we can send out invoices**

## Your challenge, should you chose to accept it...

Is to keep our customer sucess and finance teams happy
by transforming the data in `test_data.csv`, loading it into our PostgreSQL database and providing views or tables the finance and
customer sucess teams can use. They know SQL well enough to query it directly, but they can only manage simple selects - no grouping or aggregation.

Follow the `#TODO:` comments in code for some guidance on where to start... good luck!
