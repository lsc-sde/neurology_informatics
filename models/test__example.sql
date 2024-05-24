{{
    config(
        materialized = 'table',
        tags = ['test']
    )
}}

select 'Hello, World!' as Example
