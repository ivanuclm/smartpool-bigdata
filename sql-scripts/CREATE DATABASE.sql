-- CREATE DATABASE smartpool;
-- GO

USE smartpool;
GO

CREATE TABLE dbo.pools_dim (
    pool_id        INT IDENTITY(1,1) PRIMARY KEY,
    pool_name      NVARCHAR(100) NOT NULL,
    location       NVARCHAR(150),
    volume_liters  INT,
    is_heated      BIT NOT NULL DEFAULT 0,
    owner_type     NVARCHAR(50),
    updated_at     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE INDEX IX_pools_dim_updated_at ON dbo.pools_dim(updated_at);
GO

INSERT INTO dbo.pools_dim (pool_name, location, volume_liters, is_heated, owner_type) VALUES
('Piscina Casa Pueblo', 'Valdeganga (Albacete)', 30000, 0, 'private'),
('Piscina Villa Mila', 'Valdeganga (Albacete)', 700000, 0, 'private'),
('Piscina Airbnb Rural', 'Cuenca', 35000, 0, 'airbnb'),
('Piscina Hotel Centro', 'Madrid', 60000, 1, 'hotel'),
('Piscina Polideportivo Municipal', 'Ciudad Real', 80000, 1, 'sports_center');
GO


CREATE TABLE dbo.maintenance_events (
    id                 INT IDENTITY(1,1) PRIMARY KEY,
    pool_id            INT NOT NULL FOREIGN KEY REFERENCES dbo.pools_dim(pool_id),
    event_time         DATETIME2 NOT NULL,
    intervention_type  NVARCHAR(50) NOT NULL,
    product_type       NVARCHAR(50) NULL,
    product_amount     FLOAT NULL,
    notes              NVARCHAR(255) NULL,
    updated_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
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
-- Piscina Casa Pueblo (ID 1)
(1, DATEADD(DAY, -10, SYSUTCDATETIME()), 'chlorine',       'dichloro', 250, 'Tratamiento de choque tras varios días sin uso'),
(1, DATEADD(DAY, -7,  SYSUTCDATETIME()), 'ph_correction',  'minus',    150, 'Ajuste de pH tras tormenta'),
(1, DATEADD(DAY, -3,  SYSUTCDATETIME()), 'filter_backwash', NULL,      NULL,'Lavado de filtro por aumento de presión'),

-- Piscina Villa Mila (ID 2)
(2, DATEADD(DAY, -8,  SYSUTCDATETIME()), 'chlorine',       'tricloro', 200, 'Mantenimiento rutinario antes de fin de semana'),
(2, DATEADD(DAY, -4,  SYSUTCDATETIME()), 'refill',         NULL,       1500,'Relleno por evaporación y salpicaduras'),
(2, DATEADD(DAY, -1,  SYSUTCDATETIME()), 'ph_correction',  'plus',     120, 'Corrección de pH por agua demasiado ácida'),

-- Piscina Airbnb Rural (ID 3)
(3, DATEADD(DAY, -6,  SYSUTCDATETIME()), 'chlorine',       'dichloro', 220, 'Preparación antes de llegada de huéspedes'),
(3, DATEADD(DAY, -2,  SYSUTCDATETIME()), 'filter_backwash', NULL,      NULL,'Lavado de filtro tras varios días de alta ocupación'),

-- Piscina Hotel Centro (ID 4, climatizada)
(4, DATEADD(DAY, -9,  SYSUTCDATETIME()), 'chlorine',       'tricloro', 300, 'Mantenimiento semanal'),
(4, DATEADD(DAY, -5,  SYSUTCDATETIME()), 'refill',         NULL,       2500,'Relleno por pérdidas y evaporación'),
(4, DATEADD(DAY, -2,  SYSUTCDATETIME()), 'ph_correction',  'minus',    180, 'Ajuste de pH por uso intensivo'),

-- Piscina Polideportivo Municipal (ID 5, climatizada)
(5, DATEADD(DAY, -7,  SYSUTCDATETIME()), 'chlorine',       'dichloro', 400, 'Tratamiento de choque al inicio de la semana'),
(5, DATEADD(DAY, -3,  SYSUTCDATETIME()), 'filter_backwash', NULL,      NULL,'Lavado de filtro por alta turbidez'),
(5, DATEADD(DAY, -1,  SYSUTCDATETIME()), 'ph_correction',  'minus',    200, 'Corrección de pH tras sesión de aquagym');
GO

CREATE INDEX IX_maintenance_events_updated_at ON dbo.maintenance_events(updated_at);
CREATE INDEX IX_maintenance_events_pool_id_event_time ON dbo.maintenance_events(pool_id, event_time);
GO

CREATE OR ALTER TRIGGER dbo.trg_pools_dim_updated_at
ON dbo.pools_dim
AFTER UPDATE
AS
BEGIN
  SET NOCOUNT ON;
  UPDATE p
    SET updated_at = SYSUTCDATETIME()
  FROM dbo.pools_dim p
  INNER JOIN inserted i ON i.pool_id = p.pool_id;
END;
GO

CREATE OR ALTER TRIGGER dbo.trg_maintenance_events_updated_at
ON dbo.maintenance_events
AFTER UPDATE
AS
BEGIN
  SET NOCOUNT ON;
  UPDATE m
    SET updated_at = SYSUTCDATETIME()
  FROM dbo.maintenance_events m
  INNER JOIN inserted i ON i.id = m.id;
END;
GO
