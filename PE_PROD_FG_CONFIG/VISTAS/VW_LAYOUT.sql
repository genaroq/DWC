REPLACE VIEW PE_DESA_FG_CONFIG.VW_LAYOUT
AS
LOCKING ROW FOR ACCESS
SELECT
   layoutCD, 
   delimitador, 
   prefijoLayout, 
   nomTabla,
   baseDatosLayout, 
   indHeader, 
   nombreLayout,
   sufijoLayout,
   defaultDia,
   PeriodicidadCD,
   TipoCargaCD,
   RutaCD,
   Coalesce(BaseDatos,'EWA') AS BaseDatos,
   Coalesce(Comillas,'OPTIONAL ''"''') AS Comillas,
   Coalesce(IndLimpieza,'') AS IndLimpieza   
FROM PE_DESA_FG_CONFIG.TB_LAYOUT
WHERE EstadoLayout = 1;