# dbt + Jinja Q&A Cheat Sheet

## Q1. How does Jinja help in dbt?
Jinja makes SQL in dbt dynamic. It lets us reuse code, add conditions, and call dbt functions like `ref` and `source`. Without Jinja, dbt models would just be static SQL.

- `{{ ... }}` → functions / expressions → `{{ config(...) }}`, `{{ ref('table') }}`  
- `{% ... %}` → logic / flow → `{% if ... %}`, `{% for ... %}`  
- `{# ... #}` → comments  

---

## Q2. What does this code do?
```sql
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```
This tells dbt: if the model is running incrementally, only process rows with a newer `updated_at` than what’s already in the table. Otherwise, run a full load.

---

## Q3. How do we place YAML files for sources, models?
In dbt, YAML files are used for documentation, tests, and sources. The placement matters, but it’s simple.

- File name doesn’t matter: `sources.yml`, `schema.yml`, `anything.yml` all work.  
- Must live inside the `models/` folder (or a subfolder).  
- Must use `version: 2` to enable modern features like tests and docs.

Best practice: keep YAML organized (sources + models in the same folder they describe).

---

## Q4. Does dbt require us to write MERGE statements?
No. dbt doesn’t require us to manually write `MERGE`. When we configure a model as `incremental` with a `unique_key`, dbt compiles it into a warehouse-specific `MERGE` (or equivalent upsert) under the hood.

The generated SQL is stored in `target/compiled` so you can inspect what dbt actually runs.

---

## Q5. What are materializations in dbt?
- **Table** → full refresh creates a physical table  
- **View** → creates a SQL view  
- **Incremental** → MERGE/UPSERT logic for only new or changed data  
- **Ephemeral** → CTEs, compiled into downstream models (no physical object)  

---

## Q6. In dbt incremental models, why do we wrap conditions that reference `{{ this }}` inside `is_incremental()`?
Because on the very first run, `{{ this }}` (the current model’s table) doesn’t exist yet. If we reference it directly, the query fails.

- First run → `is_incremental()` = false → skip that block → full load  
- Later runs → `is_incremental()` = true → use `{{ this }}` filter → only new data  

If we skip `is_incremental()`:  
- First run → fails (table doesn’t exist)  
- Second run onwards → works the same as with `is_incremental()`

  ## Q7. What is profiles.yml
profiles.yml is a single configuration file used to define multiple environment targets such as dev, prod, and staging. It lives outside the project directory and stores connection details, so you don’t commit it to Git. Each environment is defined as an output inside the file, and you simply switch environments by running dbt build --target <env>. On local machines you typically use the dev target, while CI/CD pipelines set the target to prod. In short, one profiles.yml manages all environments, and you select which one to use at runtime.
