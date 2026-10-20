# Databricks CLI Profiles

As of 2026-10-20.

## Profiles

Three CLI profiles are set up on this laptop, all checked valid with `databricks auth profiles`. Pass one with `--profile <name>` on every command.

| Profile | Host | Signs in as | Auth | Used for |
| --- | --- | --- | --- | --- |
| `DEFAULT` | https://dbc-b5c9918e-c2d2.cloud.databricks.com (AWS Free Edition) | your AWS Free Edition user | personal access token | bundle targets `dev`, `prod` |
| `azure` | https://adb-7405618954475016.16.azuredatabricks.net (workspace `e2e-learning`, East US 2) | `keqingli1129@gmail.com` (workspace admin) | OAuth | bundle target `azure_dev`; reading `azure_prod` |
| `azure-account` | https://accounts.azuredatabricks.net (account `1d1ca9dc-3448-4226-8f99-d518d2ef65db`) | `dbadmin@keqingli1129gmail.onmicrosoft.com` (account admin) | OAuth | account console: workspaces, metastores, service principals, federation policies |

## Sign in

Each OAuth login opens a browser tab and saves the profile to `~/.databrickscfg` when you finish. Re-run the same command to sign in again when a token expires.

**`azure`: sign in with your Gmail account**

```bash
databricks auth login --host https://adb-7405618954475016.16.azuredatabricks.net --profile azure
```

**`azure-account`: sign in as `dbadmin`, never Gmail**

```bash
databricks auth login --host https://accounts.azuredatabricks.net --account-id 1d1ca9dc-3448-4226-8f99-d518d2ef65db --profile azure-account
```

1. On the Databricks "Log in" page, clear the prefilled Gmail address and type `dbadmin@keqingli1129gmail.onmicrosoft.com`.
2. On the Microsoft page, pick **Use another account** if it offers Gmail, then approve the Microsoft Authenticator prompt.
3. Wait for the success page; the CLI prints `Profile azure-account was successfully saved`.

**`DEFAULT`** uses a personal access token, so it needs no browser login.

## Check

These all passed on 2026-10-20.

```bash
databricks auth profiles                                     # all three show Valid = YES
databricks current-user me --profile azure                   # userName keqingli1129@gmail.com
databricks account workspaces list --profile azure-account   # 7405618954475016  e2e-learning  RUNNING
```

## Gotchas

- **Gmail is rejected by the account console** ("Selected user account does not exist in tenant 'Microsoft Services'"). Use `dbadmin` for `azure-account`.
- **The login tab may open in the wrong browser profile.** Copy its full address into the `dbadmin` browser window and sign in there; the callback still reaches the CLI.
- **A login can hang after the browser says success.** Stop it and run the command again; only one login can hold the callback port `localhost:8020` at a time.
- **`databricks apps logs` needs OAuth**, so it fails with `DEFAULT` (a token profile).
- **The Azure trial ends about 2026-10-28.** After that, `azure` and `azure-account` stop working unless the subscription is upgraded.
