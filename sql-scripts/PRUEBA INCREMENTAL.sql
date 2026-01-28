USE smartpool;
GO

INSERT INTO dbo.pools_dim (pool_name, location, volume_liters, is_heated, owner_type)
VALUES ('Piscina Prueba Incremental', 'Albacete', 42000, 1, 'private');
GO

INSERT INTO dbo.maintenance_events (
    pool_id,
    event_time,
    intervention_type,
    product_type,
    product_amount,
    notes
)
VALUES
(1, SYSUTCDATETIME(), 'chlorine', 'dichloro', 180, 'Evento incremental de prueba 1'),
(1, DATEADD(MINUTE, 2, SYSUTCDATETIME()), 'ph_correction', 'minus', 90, 'Evento incremental de prueba 2');
GO
