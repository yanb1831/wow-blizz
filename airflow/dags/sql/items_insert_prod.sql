INSERT INTO prod.items (created_at, id, name, level, required_level)
SELECT created_at, id::int, name, level::int, required_level::int
FROM stage.items
WHERE (id, name, level, required_level) IS NOT NULL
AND created_at::date = '{{ ds }}'
ORDER BY 1;