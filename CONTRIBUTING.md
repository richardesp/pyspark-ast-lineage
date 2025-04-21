# **Contributing to pyspark-ast-lineage**

Thank you for considering contributing to `pyspark-ast-lineage`. This guide outlines the process for contributing to ensure smooth and efficient collaboration.

---

## **Table of Contents**  
1. [Setting Up the Development Environment](#setting-up-the-development-environment)  
2. [Branching Strategy](#branching-strategy)  
3. [Commit Messages](#commit-messages)  
4. [Development Workflow](#development-workflow)  
5. [Pull Requests](#pull-requests)  
6. [Releases](#releases)  
7. [Code of Conduct](#code-of-conduct)  

---

## **Setting Up the Development Environment**

This section explains how to configure the local development environment using `uv` for dependency management.

### **Prerequisites**  
Ensure you have the following installed:  
- Python **3.9+**  
- `uv` (dependency management)  

### **Installing `uv` and Setting Up the Virtual Environment**  

#### **1. Install `uv` (if not installed)**  
```bash
pip install uv
```

#### **2. Create and Activate the Virtual Environment**  
```bash
uv venv
source .venv/bin/activate  # macOS/Linux
.venv\Scripts\activate     # Windows (PowerShell)
```

#### **3. Install Dependencies**  
- To install **main dependencies**, run:
  ```bash
  uv pip install -r pyproject.toml
  ```

- To install **development dependencies** (for linting, testing, etc.), run:
  ```bash
  uv pip install -r pyproject.toml --extra dev 
  ```

- Alternatively, you can install **all dependencies** (main + dev) at once:
  ```bash
  uv pip install -r pyproject.toml --all-extras
  ```

---

### **Configuring the IDE for Autocompletion**  

To enable autocompletion and type hints, configure your IDE to use the `uv` virtual environment:

#### **VS Code**  
1. Open `Command Palette` (`Cmd+Shift+P` or `Ctrl+Shift+P`).  
2. Search for `Python: Select Interpreter`.  
3. Select `.venv` created by `uv`.  

#### **PyCharm**  
1. Go to `Preferences` → `Project: <project-name>` → `Python Interpreter`.  
2. Click `Add Interpreter` → `Existing Environment`.  
3. Select `.venv` as the interpreter.  

---

### **Running Code Checks and Formatting**  

#### **Run Formatters & Linters**  
Before committing code, run the following:  

```bash
uv run pytest  # Run tests
uv run ruff check --fix .  # Lint the code
uv run black .  # Format the code
```

#### **Set up pre-commit hooks** (optional but recommended for code quality and CI/CD workflows):
```bash
uv run pre-commit install
```

To manually trigger pre-commit on all files:  

```bash
uv run pre-commit run --all-files
```

---

### **Summary of Commands**

| Task                          | Command |
|--------------------------------|---------|
| Install main dependencies     | `uv pip install -r pyproject.toml` |
| Install development dependencies | `uv pip install -r pyproject.toml --extra dev` |
| Install all dependencies (main + dev) | `uv pip install -r pyproject.toml --all-extras` |
| Activate virtual environment  | `source .venv/bin/activate` (Mac/Linux) / `.venv\Scripts\activate` (Windows) |
| Run tests                     | `uv run pytest` |
| Run linters and formatters    | `uv run ruff check --fix . && uv run black .` |
| Install pre-commit hooks      | `uv run pre-commit install` |
| Run pre-commit on all files   | `uv run pre-commit run --all-files` |

---

## **Branching Strategy**

This project follows a structured branching model:

- **`main`**: The production-ready branch containing only stable releases.
- **`develop`**: The main development branch where all new features, fixes, and changes are merged.
- **Feature Branches**: Created for specific tasks using the following naming conventions:
  - `feat/feature-name` for new features for extractors, evaluators, etc..
  - `fix/bug-name` for bug fixes.
  - `chore/task-name` for maintenance or tooling updates.
  - `docs/documentation-name` for documentation updates.

---

## **Commit Messages**

We follow [Conventional Commits](https://www.conventionalcommits.org/) to maintain a structured commit history.

### **Commit Format**  
```bash
<type>: <description>
```

### **Commit Types**  
- `feat`: A new feature.
- `fix`: A bug fix.
- `docs`: Documentation updates.
- `chore`: Maintenance tasks (e.g., dependency updates).
- `test|tests`: Any new test or update to the existing ones.

### **Examples**  
```bash
feat: add user authentication  
fix: resolve issue with data validation  
docs: update README.md with new setup instructions  
```

---

## **Development Workflow**

### **1. Cloning the Repository**  
```bash
git clone https://github.com/richardesp/pyspark-ast-lineage.git
cd pyspark-ast-lineage
```

### **2. Creating a New Branch**  
```bash
git checkout -b feat/your-feature-name
```

### **3. Making Changes and Committing**  
```bash
git add .
git commit -m "feat: describe your change"
```

### **4. Syncing with `develop`**  
```bash
git pull origin develop
```

### **5. Testing and Code Checks**  
Before submitting a pull request, ensure your code passes all tests and linting checks.

---

## **Pull Requests**

All new contributions must go through a pull request (PR) process:

1. **Target Branch**: PRs must target the `develop` branch.
2. **Description**: Provide a clear summary of the changes in the PR.
3. **Checklist Before Submitting**:
    - Code follows project guidelines.
    - Commits follow the Conventional Commit format.
    - Linting and formatting checks pass.
    - All necessary tests are included.
4. **Review Process**: PRs will be reviewed and may require changes before merging.

---

## **Releases**

Releases follow **Semantic Versioning**:
- **Major**: Breaking changes.
- **Minor**: New features, backward-compatible.
- **Patch**: Bug fixes.

### **Release Process**
1. The `develop` branch is merged into `main`.
2. The release is tagged, e.g., `v1.0.0`.
3. The commit message follows the format:  
   ```bash
   chore(release): v1.0.0
   ```

---

## **Code of Conduct**

By contributing, you agree to follow the project's Code of Conduct to ensure a welcoming and inclusive environment.

---

## **Questions and Support**

For any questions, open an issue or contact the maintainers:

- GitHub: [@richardesp](https://github.com/richardesp)

Thank you for contributing!