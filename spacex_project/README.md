{% docs __overview__ %}

# SpaceX Data Model

This dbt project transforms raw SpaceX API data into a dimensional model optimized for analytics.

## Model Structure

1. Staging Models (views)

   - Basic transformations and type casting
   - One-to-one relationship with source tables

2. Intermediate Models (incremental)

   - Bridge tables for many-to-many relationships
   - Precalculated aggregates

3. Mart Models (tables)
   - Dimension tables for core entities
   - Fact tables for launches and costs

## Usage

To run the entire project:

```bash
dbt run
dbt test
```

To run and test a specific models and his upstream models

```bash
dbt build --select +marts.pbl_spacex_data_fct__launches
```

### Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
