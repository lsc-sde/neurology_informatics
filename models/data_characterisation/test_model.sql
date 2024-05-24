select *
from {{ source('omop', 'person') }}