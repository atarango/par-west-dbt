{% macro email_key(col) -%}
-- Canonical email key (BigQuery-safe).
-- Gmail/Googlemail: strip +tags and dots in local-part and force domain to gmail.com.
-- Others: lowercase, strip +tags, keep dots.
case
  when {{ col }} is null or {{ col }} = '' then null
  when lower(split({{ col }},'@')[safe_offset(1)]) in ('gmail.com','googlemail.com') then
    concat(
      regexp_replace(split(lower(split({{ col }},'@')[safe_offset(0)]), '+')[safe_offset(0)], r'\.', ''),
      '@gmail.com'
    )
  else
    concat(
      split(lower(split({{ col }},'@')[safe_offset(0)]), '+')[safe_offset(0)],
      '@',
      lower(split({{ col }},'@')[safe_offset(1)])
    )
end
{%- endmacro %}
