# Contract tests & deploy profiles

- Plan: [TEST_PLAN.md](TEST_PLAN.md)
- Groups/profiles: [`../profiles.yaml`](../profiles.yaml)

## Profiles (reuse groups, change URL)

| Profile                    | Groups                            | CI default?                           |
|----------------------------|-----------------------------------|---------------------------------------|
| `ci-lite`                  | unit, java, contract              | **Yes**                               |
| `docker-lite` / `k8s-lite` | contract, lite_api                | post-deploy                           |
| `docker-full` / `k8s-full` | contract, **e2e_full** (required) | post-deploy / nightly — **not** PR CI |
| `logic`                    | unit, java                        | offline                               |

`e2e_full` is intentionally **out of** `ci-lite`, but **required** for Full deploy-health evidence (`docker-full`/
`k8s-full`).

```bash
# list 
make test-profiles

# PR / CI
make test-report PROFILE=ci-lite

# after Lite compose or k8s Lite port-forward  
FLUXMETER_API=http://127.0.0.1:8000 make test-report PROFILE=docker-lite

# after Full stack (compose or k8s) - includes e2e 
FLUXMETER_API=http://127.0.0.1:8000 make test-report PROFILE=docker-full
FLUXMETER_API=https://… 

make test-report PROFILE=k8s-full
```

Reports: `reports/logic-report.json`, `reports/deploy-health.json`, plus per-group JUnit XML.