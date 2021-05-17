/*************************************************************************************************************************************
* PROYECTO        : Framework Generico de Extraccion y Carga de Informacion DWC (Python) 
* FUNCION         : Generar informacion de la tabla de Instancias de Ejecucion o Agendamiento,
*                   considerando procesos con predecesor y sin predecesor.
* TABLA(S) BASE   : PE_PROD_FG_CONFIG.SP_GM_Schedule
* NOTAS           : 
* AUTOR           : TdP Pedro Moreno Ampuero PROD
* BITACORA DE CAMBIOS
* VERSION         FECHA          HECHO POR           COMENTARIOS
* ----------------------------------------------------------------------------------------------------------------------------------------
*     1.0    15/07/2019   TDP Pedro Moreno       Version Inicial
*     1.1    16/12/2019   TDP Genaro Quinteros   Ajuste proceso horario
*     1.2    17/12/2019   TDP Pedro Moreno       Ajuste para multiejecucion mensual de procesos (+ de 1 vez)
*     1.3    24/07/2020   TDP Pedro Moreno       Ajuste que evita un loop infinito debido a los procesos mensuales y manejo de flags (v_loop)
*     1.4    31/07/2020   TDP Pedro Moreno       Ajuste en registro de logs y se adiciona nroregs de ejecucion anterior y posterior
*     1.5    19/01/2021   TDP Genaro Quinteros   Se agrego Ejecución Ultimo dia Mes y Ejecucion en varios dias de la semana
* ----------------------------------------------------------------------------------------------------------------------------------------
* MODO DE EJECUCION :
* PE_DESA_FG_CONFIG.SP_Genera_Matriz_Agendamiento('PE_DESA_FG_CONFIG','TB_SCHEDULE','PE_DESA_FG_CONFIG','TB_SCH_Log','20190724')
*****************************************************************************************************************************************
* PARAMETROS: 
*    V_BDGestorConf =   PE_DESA_FG_CONFIG
*    V_TablaInput   =   TB_SCHEDULE
*    V_BDLog        =   PE_DESA_FG_CONFIG
*    V_TablaLog     =   TB_SCH_Log
******************************************************************************************************************************************/
REPLACE PROCEDURE PE_DESA_FG_CONFIG.SP_GM_Schedule
(
  IN V_BDGestorConf VARCHAR(30)
  , IN V_TablaInput VARCHAR(30)
  , IN V_BDLog VARCHAR(30)
  , IN V_TablaLog VARCHAR(30)
  , IN V_Fecha VARCHAR(8)  
)
                                                                 
BEGIN
  DECLARE MySQLCode INTEGER;
  DECLARE qryUpdLog,v_ValorFrecuenciaAdic VARCHAR(1500);
  DECLARE xFechaProceso, xHorFin, xMMSS VARCHAR(8);
  DECLARE xHorMaxEjec, xHorIni TIME(6);
  DECLARE xFecIni DATE;
  DECLARE xFecIniEjec_Ts, xFecFinEjec_Ts, xFecCreaTS TIMESTAMP(6);
  DECLARE xPatron, xSchdMatrixCd, xScheduleCD, xSchdMatrixCDPred, xRegistrosAyer, xRegsAntes, xRegsDespues INTEGER;
  DECLARE xEstadoCD, xInicioContador, xFinContador, xDiaSemana, xNumEjec SMALLINT;
  DECLARE V_Id_Agendamiento VARCHAR(20);
  DECLARE xFlagBusqueda, iContador INTEGER;
  DECLARE v_sw ,v_loop SMALLINT;
  DECLARE V_SQLText VARCHAR(4000);
  DECLARE V_SP VARCHAR(100);
  DECLARE V_CRegExistente CURSOR FOR SRegExistente;
  -------------------------------------------------------------------------------------------
  -- MANEJO DE ERRORES DE LA EJECUCION
  --------------------------------------------------------------------------------------------
  DECLARE EXIT HANDLER
  FOR SqlException 
  BEGIN
    SET MySQLCode = SqlCode;
    SET qryUpdLog =
    'UPDATE ' || V_BDGestorConf || '.' || V_TablaLog ||
    ' SET CodigoSQL = ' || Cast(MySQLCode AS VARCHAR(10)) || ', Fecha_Ejecucion_Fin = CURRENT_DATE, Hora_Ejecucion_Fin = CURRENT_TIME(0)' ||
    ', Descripcion_Error = (SELECT x.ErrorText FROM DBC.ErrorMsgs x WHERE x.ErrorCode = ' || Cast(MySQLCode AS VARCHAR(20)) || ')' ||
    ', Estado_Ejecucion = ''54''' || ' WHERE Id_Agendamiento = ' || V_Id_Agendamiento || ';';
    CALL DBC.SysExecSQL(qryUpdLog);
  END;

  -- Registro de Ejecucion de Proceso
  ---------------------------------------
  SELECT Cast(Coalesce(Max(t.Id_Agendamiento),0) + 1 AS VARCHAR(10))
    INTO V_Id_Agendamiento
    FROM PE_DESA_FG_CONFIG.TB_SCH_Log t    
   ORDER BY 1 DESC;

  -- Conteo de Nro Registros de Antes de Ejecucion
  SELECT Coalesce(Count(1),0) + 1
    INTO xRegsAntes
    FROM PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ t;

  SET qryUpdLog = 'INSERT INTO ' || V_BDGestorConf || '.' || V_TablaLog || ' (Id_Agendamiento, Fecha_Ejecucion_Inicio, Hora_Ejecucion_Inicio, Estado_Ejecucion, Descripcion_Error, Nro_Registros_Antes)' ||
  ' VALUES (' || V_Id_Agendamiento || ', CURRENT_DATE, Current_Time(0), ''52'', ''En Ejecucion'', ' || Cast( xRegsAntes AS VARCHAR(10)) || ');';
  CALL DBC.SysExecSQL(qryUpdLog);
  
  SET V_SP = 'SP_GM_Schedule';
  
  -------------------------------------------------------------------------------------------
  -- SELECCION DE COLUMNAS DE TABLA STAGE
  --------------------------------------------------------------------------------------------
  SET xPatron = Cast(To_Char(Current_Timestamp, 'YYMMDD') AS INTEGER)*10000;
  
  SELECT Coalesce(Max(t.SchdMatrixCd + 1), Cast(To_Char(Current_Timestamp, 'YYMMDDHHMI') AS INTEGER))
    INTO xSchdMatrixCd
    --FROM PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ t
    FROM PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ t    
   WHERE t.SchdMatrixCD >= xPatron
   ORDER BY 1 DESC;

  SET xFechaProceso = Cast(To_Char(Current_Timestamp, 'YYYYMMDD') AS VARCHAR(8));
  SET xFecIniEjec_Ts = Cast(NULL AS TIMESTAMP(6));
  SET xFecFinEjec_Ts = Cast(NULL AS TIMESTAMP(6));
  SET xEstadoCD = Cast(51 AS SMALLINT);
  SET xNumEjec = 0;
  SET xFecIni = Cast(V_Fecha AS DATE FORMAT 'YYYYMMDD');
   
  INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ_HIST
  SELECT * FROM PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ;
  
  -- Limpiamos Schedule_matriz
  DELETE PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ;
  
  /*
  ====================================================================
  03/12/2019 Pedro Moreno ::
  Se comentan estas lineas porque al final de este SP se insertan los 
  registros pendientes de ejecutar. 
  ====================================================================
  DELETE PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ
   WHERE EstadoCD = 53       -- Estado Terminado
      OR horMaxEjec > HorIni         
      OR FecIni < DATE - 2;
  */
  
  -- Fin de limpieza
  
  SET iContador = 1;
  
  FOR Reg AS NoPredecesor CURSOR FOR
  SELECT
    ScheduleCd,
    t.TipSchdCd AS TipoScheduleCd,
    t.ScheduleCdPred AS ScheduleCdPre,
    t.LayoutCd,
    t.UnidadFrecCd AS UnidadFrecuenciaCd,
    t.FecIni AS FechaInicio,
    t.HorIni AS HoraInicio,
    t.HorFin AS HoraFinalizacion,
    t.FlagIndFin AS FlagIndFinalizacion,
    t.HorMaxDura AS DuracionMaxima,
    t.ValorFrecuencia AS ValorFrecuencia,
    t.ValorFrecuenciaAdic AS ValorFrecuenciaAdic
    FROM PE_DESA_FG_CONFIG.TB_SCHEDULE t
   WHERE t.ScheduleCdPred = 0
     AND T.Estado = 1
   ORDER BY t.ScheduleCd ASC
   
  DO
      
    IF To_Char(Reg.FechaInicio, 'YYYYMMDD') <= To_Char(Current_Date, 'YYYYMMDD') THEN

       IF Reg.UnidadFrecuenciaCd = 101 THEN -- Diario
          
          SET xScheduleCD = Reg.ScheduleCD;
          SET xSchdMatrixCDPred = Reg.ScheduleCdPre;
          SET xHorIni = Reg.HoraInicio;
          SET xHorMaxEjec = Reg.HoraFinalizacion;
          SET xFecCreaTS = Cast(Current_Timestamp AS TIMESTAMP(6));

          SET V_SQLText = 'INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ VALUES ( ' ||
          Cast(xSchdMatrixCD AS VARCHAR(10)) || ',' || Cast(xScheduleCD AS VARCHAR(10)) || ',' || Cast(xSchdMatrixCDPred AS VARCHAR(20)) || ',''' ||
          Coalesce(To_Char(xFecIniEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || Coalesce(To_Char(xFecFinEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || To_Char(xFecIni,'YYYY-MM-DD') || ''',''' || Cast(xHorIni AS VARCHAR(15)) || ''',''' ||
          Cast(xHorMaxEjec AS VARCHAR(15)) || ''',''' || To_Char(xFecCreaTS,'YYYY-MM-DD HH24:MI:SSFF') || ''',' || Cast(xEstadoCD AS VARCHAR(4)) || ',' || Coalesce(Cast(xNumEjec AS VARCHAR(4)),'NULL') || ');';
             
          INSERT INTO PE_DESA_FG_CONFIG.LOGS_SPS_DETAILS
          VALUES (V_SP, iContador, Current_Timestamp, V_SQLText);
             
          INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ
          VALUES ( :xSchdMatrixCD, :xScheduleCD, :xSchdMatrixCDPred,
                   :xFecIniEjec_TS, :xFecFinEjec_TS, :xFecIni,
                   :xHorIni, :xHorMaxEjec, :xFecCreaTS, :xEstadoCD, :xNumEjec);

          SET iContador = iContador + 1;

       ELSEIF Reg.UnidadFrecuenciaCd = 102 THEN -- Mensual
          SET v_ValorFrecuenciaAdic = Reg.ValorFrecuenciaAdic;
		  
		  SELECT Instr(v_ValorFrecuenciaAdic,'31')   -- Pregunto si se ejecuta último dia de mes
            INTO xFlagBusqueda;
		  
		  IF xFlagBusqueda > 0 THEN
		     SELECT OReplace(v_ValorFrecuenciaAdic,'31',To_Char(Last_Day(DATE),'DD'))
			   INTO v_ValorFrecuenciaAdic;
		  END IF;
		  
          SELECT Instr(v_ValorFrecuenciaAdic,Substr(xFechaProceso,7,2))
            INTO xFlagBusqueda;
                
          IF xFlagBusqueda > 0 THEN
             SET xScheduleCD = Reg.ScheduleCD;
             SET xSchdMatrixCDPred = Reg.ScheduleCdPre;
             SET xHorIni = Reg.HoraInicio;
             SET xHorMaxEjec = Reg.HoraFinalizacion;
             SET xFecCreaTS = Cast(Current_Timestamp AS TIMESTAMP(6));

             SET V_SQLText = 'INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ VALUES ( ' ||
             Cast(xSchdMatrixCD AS VARCHAR(10)) || ',' || Cast(xScheduleCD AS VARCHAR(10)) || ',' || Cast(xSchdMatrixCDPred AS VARCHAR(20)) || ',''' ||
             Coalesce(To_Char(xFecIniEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || Coalesce(To_Char(xFecFinEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || To_Char(xFecIni,'YYYY-MM-DD') || ''',''' || Cast(xHorIni AS VARCHAR(15)) || ''',''' ||
             Cast(xHorMaxEjec AS VARCHAR(15)) || ''',''' || To_Char(xFecCreaTS,'YYYY-MM-DD HH24:MI:SSFF') || ''',' || Cast(xEstadoCD AS VARCHAR(4)) || ',' || Coalesce(Cast(xNumEjec AS VARCHAR(4)),'NULL') || ');';
             
             INSERT INTO PE_DESA_FG_CONFIG.LOGS_SPS_DETAILS
             VALUES (V_SP, iContador, Current_Timestamp, V_SQLText);
             
             INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ
             VALUES ( :xSchdMatrixCD, :xScheduleCD, :xSchdMatrixCDPred,
                      :xFecIniEjec_TS, :xFecFinEjec_TS, :xFecIni,
                      :xHorIni, :xHorMaxEjec, :xFecCreaTS, :xEstadoCD, :xNumEjec );

             SET iContador = iContador + 1;

          END IF;
                
       ELSEIF Reg.UnidadFrecuenciaCd = 103 THEN -- Horario
                
          SELECT Cast(Substr(Cast(Reg.HoraInicio AS VARCHAR(8)),1,2) AS SMALLINT)
            INTO xInicioContador;
                  
          SET xMMSS = Substr(Cast(Reg.HoraInicio AS VARCHAR(8)),3);
          
          SELECT Cast(Substr(Cast(Reg.HoraFinalizacion AS VARCHAR(8)),1,2) AS SMALLINT)
            INTO xFinContador;
                  
          WHILE (xInicioContador <= xFinContador)
          DO
             BEGIN
                SET xScheduleCD = Reg.ScheduleCD;
                SET xSchdMatrixCDPred = Reg.ScheduleCdPre;
                SET xHorIni = Cast(Cast(LPad(Trim(xInicioContador),2,'0') || xMMSS AS TIME(0)) AS TIME(6));
                SET xHorMaxEjec = Reg.HoraFinalizacion;
                SET xFecCreaTS = Cast(Current_Timestamp AS TIMESTAMP(6));

                SET V_SQLText = 'INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ VALUES ( ' ||
                Cast(xSchdMatrixCD AS VARCHAR(10)) || ',' || Cast(xScheduleCD AS VARCHAR(10)) || ',' || Cast(xSchdMatrixCDPred AS VARCHAR(20)) || ',''' ||
                Coalesce(To_Char(xFecIniEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || Coalesce(To_Char(xFecFinEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || To_Char(xFecIni,'YYYY-MM-DD') || ''',''' || Cast(xHorIni AS VARCHAR(15)) || ''',''' ||
                Cast(xHorMaxEjec AS VARCHAR(15)) || ''',''' || To_Char(xFecCreaTS,'YYYY-MM-DD HH24:MI:SSFF') || ''',' || Cast(xEstadoCD AS VARCHAR(4)) || ',' || Coalesce(Cast(xNumEjec AS VARCHAR(4)), 'NULL') || ');';

                INSERT INTO PE_DESA_FG_CONFIG.LOGS_SPS_DETAILS
                VALUES (V_SP, iContador, Current_Timestamp, V_SQLText);
                
                INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ
                VALUES ( :xSchdMatrixCD, :xScheduleCD, :xSchdMatrixCDPred,
                :xFecIniEjec_TS, :xFecFinEjec_TS, :xFecIni,
                :xHorIni, :xHorMaxEjec, :xFecCreaTS, :xEstadoCD, :xNumEjec );

                SET xInicioContador = xInicioContador + reg.ValorFrecuencia;
                
                IF xInicioContador <= 23 THEN
                   SET xHorIni = Cast(Cast(LPad(Trim(xInicioContador),2,'0') || xMMSS AS TIME(0)) AS TIME(6));
                   SET xSchdMatrixCD = xSchdMatrixCD +1;      
                   SET iContador = iContador + 1;
                ELSE 
                   SET iContador = 24;
                END IF;

             END;
             
          END WHILE;
         
       ELSEIF Reg.UnidadFrecuenciaCd = 104 THEN -- Semanal
                
          SELECT Cast(day_of_week AS SMALLINT)
            INTO xDiaSemana
            FROM SYS_CALENDAR.CALENDAR
           WHERE Calendar_Date = Current_Date;
       
       -- Agregado por GQC 20210106 ****************************************
          SET v_ValorFrecuenciaAdic = Reg.ValorFrecuenciaAdic; 
          
          SELECT Instr(v_ValorFrecuenciaAdic , Cast(xDiaSemana AS VARCHAR(2))) INTO xFlagBusqueda;
                                
          --IF xDiaSemana = Reg.ValorFrecuencia THEN 
          IF xDiaSemana = Reg.ValorFrecuencia OR xFlagBusqueda > 0 THEN 
       -- Fin cambios    20210106 ******************************************

             SET xScheduleCD = Reg.ScheduleCD;
             SET xSchdMatrixCDPred = Reg.ScheduleCdPre;
             SET xHorIni = Reg.HoraInicio;
             SET xHorMaxEjec = Reg.HoraFinalizacion;
             SET xFecCreaTS = Cast(Current_Timestamp AS TIMESTAMP(6));

             SET V_SQLText = 'INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ VALUES ( ' ||
             Cast(xSchdMatrixCD AS VARCHAR(10)) || ',' || Cast(xScheduleCD AS VARCHAR(10)) || ',' || Cast(xSchdMatrixCDPred AS VARCHAR(20)) || ',''' ||
             Coalesce(To_Char(xFecIniEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || Coalesce(To_Char(xFecFinEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || To_Char(xFecIni,'YYYY-MM-DD') || ''',''' || Cast(xHorIni AS VARCHAR(15)) || ''',''' ||
             Cast(xHorMaxEjec AS VARCHAR(15)) || ''',''' || To_Char(xFecCreaTS,'YYYY-MM-DD HH24:MI:SSFF') || ''',' || Cast(xEstadoCD AS VARCHAR(4)) || ',' || Coalesce(Cast(xNumEjec AS VARCHAR(4)),'NULL') || ');';

             INSERT INTO PE_DESA_FG_CONFIG.LOGS_SPS_DETAILS
             VALUES (V_SP, iContador, Current_Timestamp, V_SQLText);
             
             INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ
             VALUES ( :xSchdMatrixCD, :xScheduleCD, :xSchdMatrixCDPred,
                      :xFecIniEjec_TS, :xFecFinEjec_TS, :xFecIni,
                      :xHorIni, :xHorMaxEjec, :xFecCreaTS, :xEstadoCD, :xNumEjec );

             SET iContador = iContador + 1;

          END IF;
       END IF;
    END IF;
    
    SET xSchdMatrixCD = xSchdMatrixCD + 1;
    
 END FOR;
 
-- Loop para los que tienen Predecesor    
 SET v_sw = 0;

 WHILE (v_sw = 0) 
 DO
 
    SET v_loop = 0;
    
    FOR Reg AS NoPredecesor CURSOR FOR
    WITH q1
    AS
    ( SELECT
        t.ScheduleCd,
        t.TipSchdCd AS TipoScheduleCd,
        t2.SchdMatrixCd AS ScheduleCdPre,
		t.ScheduleCdPred SchdCdPre,
        t.LayoutCd,
        t.UnidadFrecCd AS UnidadFrecuenciaCd,
        t.FecIni AS FechaInicio,
        t.HorIni AS HoraInicio,
        t.HorFin AS HoraFinalizacion,
        t.FlagIndFin AS FlagIndFinalizacion,
        t.HorMaxDura AS DuracionMaxima,
        t.ValorFrecuencia AS ValorFrecuencia,
        t.ValorFrecuenciaAdic AS ValorFrecuenciaAdic
        FROM PE_DESA_FG_CONFIG.TB_SCHEDULE T
        LEFT JOIN PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ t2
          ON t2.ScheduleCd = t.ScheduleCdPred 
             AND t.horIni = t2.horIni
       WHERE t.ScheduleCdPred > 0
         AND t2.SchdMatrixCdPred >= 0
         AND T.Estado = 1
    ),
    q2
    AS
    ( SELECT t.*
        , Instr(Coalesce(ValorFrecuenciaAdic,Cast(ValorFrecuencia AS VARCHAR(3))),Substr(Current_Date (FORMAT 'YYYYMMDD') (VARCHAR(8)),7,2)) FlagValidacion
        FROM PE_DESA_FG_CONFIG.TB_SCHEDULE t
       WHERE UnidadFrecCd = 102
         AND Estado = 1
         AND FlagValidacion = 0
    )
    SELECT q1.*
      FROM q1
     WHERE NOT EXISTS (SELECT 1 FROM PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ t3 WHERE q1.ScheduleCd = t3.ScheduleCd )
       AND NOT EXISTS (SELECT 1 FROM q2 WHERE q2.ScheduleCd = q1.ScheduleCd)
     ORDER BY q1.ScheduleCd ASC

    DO
        SET v_loop = 1;
        
        IF Reg.FechaInicio <= xFecIni THEN
      
             IF Reg.UnidadFrecuenciaCd = 101 THEN -- Diario
                
                SET xScheduleCD = Reg.ScheduleCD;
                SET xSchdMatrixCDPred = Reg.ScheduleCdPre;
                
                SET xHorIni = Reg.HoraInicio;
                SET xHorMaxEjec = Reg.HoraFinalizacion;
                SET xFecCreaTS = Cast(Current_Timestamp AS TIMESTAMP(6));

                SET V_SQLText = 'INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ VALUES ( ' ||
                Cast(xSchdMatrixCD AS VARCHAR(10)) || ',' || Cast(xScheduleCD AS VARCHAR(10)) || ',' || Cast(xSchdMatrixCDPred AS VARCHAR(20)) || ',''' ||
                Coalesce(To_Char(xFecIniEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || Coalesce(To_Char(xFecFinEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || To_Char(xFecIni,'YYYY-MM-DD') || ''',''' || Cast(xHorIni AS VARCHAR(15)) || ''',''' ||
                Cast(xHorMaxEjec AS VARCHAR(15)) || ''',''' || To_Char(xFecCreaTS,'YYYY-MM-DD HH24:MI:SSFF') || ''',' || Cast(xEstadoCD AS VARCHAR(4)) || ',' || Coalesce(Cast(xNumEjec AS VARCHAR(4)),'NULL') || ');';
                 
                INSERT INTO PE_DESA_FG_CONFIG.LOGS_SPS_DETAILS
                VALUES (V_SP, iContador, Current_Timestamp, V_SQLText);

                INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ
                VALUES ( :xSchdMatrixCD, :xScheduleCD, :xSchdMatrixCDPred,
                         :xFecIniEjec_TS, :xFecFinEjec_TS, :xFecIni,
                         :xHorIni, :xHorMaxEjec, :xFecCreaTS, :xEstadoCD, :xNumEjec );
                 
                SET iContador = iContador + 1;
                 
             ELSEIF Reg.UnidadFrecuenciaCd = 102 THEN -- Mensual
		        SET v_ValorFrecuenciaAdic = Reg.ValorFrecuenciaAdic;
				  
				SELECT Instr(v_ValorFrecuenciaAdic,'31')   -- Pregunto si se ejecuta último dia de mes
		          INTO xFlagBusqueda;
				  
				IF xFlagBusqueda > 0 THEN
				   SELECT OReplace(v_ValorFrecuenciaAdic,'31',To_Char(Last_Day(DATE),'DD'))
				     INTO v_ValorFrecuenciaAdic;
				END IF;
				
		        SELECT Instr(v_ValorFrecuenciaAdic,Substr(xFechaProceso,7,2))
		          INTO xFlagBusqueda;
			 
                IF xFlagBusqueda > 0 THEN
                   SET xScheduleCD = Reg.ScheduleCD;
                   SET xSchdMatrixCDPred = Reg.ScheduleCdPre;
                   SET xHorIni = Reg.HoraInicio;
                   SET xHorMaxEjec = Reg.HoraFinalizacion;
                   SET xFecCreaTS = Cast(Current_Timestamp AS TIMESTAMP(6));

                   SET V_SQLText = 'INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ VALUES ( ' ||
                   Cast(xSchdMatrixCD AS VARCHAR(10)) || ',' || Cast(xScheduleCD AS VARCHAR(10)) || ',' || Cast(xSchdMatrixCDPred AS VARCHAR(20)) || ',''' ||
                   Coalesce(To_Char(xFecIniEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || Coalesce(To_Char(xFecFinEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || To_Char(xFecIni,'YYYY-MM-DD') || ''',''' || Cast(xHorIni AS VARCHAR(15)) || ''',''' ||
                   Cast(xHorMaxEjec AS VARCHAR(15)) || ''',''' || To_Char(xFecCreaTS,'YYYY-MM-DD HH24:MI:SSFF') || ''',' || Cast(xEstadoCD AS VARCHAR(4)) || ',' || Coalesce(Cast(xNumEjec AS VARCHAR(4)),'NULL') || ');';
                 
                   INSERT INTO PE_DESA_FG_CONFIG.LOGS_SPS_DETAILS
                   VALUES (V_SP, iContador, Current_Timestamp, V_SQLText);

                   INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ
                    VALUES ( :xSchdMatrixCD, :xScheduleCD, :xSchdMatrixCDPred,
                             :xFecIniEjec_TS, :xFecFinEjec_TS, :xFecIni,
                             :xHorIni, :xHorMaxEjec, :xFecCreaTS, :xEstadoCD, :xNumEjec );
                                
                   SET iContador = iContador + 1;
                       
                END IF;
                
             ELSEIF Reg.UnidadFrecuenciaCd = 103 THEN -- Horario
                    
                SELECT Cast(Substr(Cast(Reg.HoraInicio AS VARCHAR(8)),1,2) AS SMALLINT)
                  INTO xInicioContador;
                
                SET xMMSS = Substr(Cast(Reg.HoraInicio AS VARCHAR(8)),3);
                
                SELECT Cast(Substr(Cast(Reg.HoraFinalizacion AS VARCHAR(8)),1,2) AS SMALLINT)
                  INTO xFinContador;
                
                SET xHorIni =  Reg.HoraInicio;
                
                WHILE (xInicioContador <= xFinContador)
                DO
                   BEGIN
                     
                    SET xScheduleCD = Reg.ScheduleCD;
                     
                    SELECT SchdMatrixCD INTO xSchdMatrixCDPred
                      FROM PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ 
                     WHERE SCheduleCD =  Reg.SchdCdPre
                       AND HorIni = :xHorIni;
                             
                    SET xHorMaxEjec = Reg.HoraFinalizacion;
                    SET xFecCreaTS = Cast(Current_Timestamp AS TIMESTAMP(6));

                    SET V_SQLText = 'INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ VALUES ( ' ||
                    Cast(xSchdMatrixCD AS VARCHAR(10)) || ',' || Cast(xScheduleCD AS VARCHAR(10)) || ',' || Cast(xSchdMatrixCDPred AS VARCHAR(20)) || ',''' ||
                    Coalesce(To_Char(xFecIniEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || Coalesce(To_Char(xFecFinEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || To_Char(xFecIni,'YYYY-MM-DD') || ''',''' || Cast(xHorIni AS VARCHAR(15)) || ''',''' ||
                    Cast(xHorMaxEjec AS VARCHAR(15)) || ''',''' || To_Char(xFecCreaTS,'YYYY-MM-DD HH24:MI:SSFF') || ''',' || Cast(xEstadoCD AS VARCHAR(4)) || ',' || Coalesce(Cast(xNumEjec AS VARCHAR(4)),'NULL') || ');';
                 
                    INSERT INTO PE_DESA_FG_CONFIG.LOGS_SPS_DETAILS
                    VALUES (V_SP, iContador, Current_Timestamp, V_SQLText);

                    INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ
                    VALUES ( :xSchdMatrixCD, :xScheduleCD, :xSchdMatrixCDPred,
                             :xFecIniEjec_TS, :xFecFinEjec_TS, :xFecIni,
                             :xHorIni, :xHorMaxEjec, :xFecCreaTS, :xEstadoCD, :xNumEjec );
                          
                    SET xInicioContador = xInicioContador + reg.ValorFrecuencia;
                    
                    IF xInicioContador <= 23 THEN
                       SET xHorIni = Cast(Cast(LPad(Trim(xInicioContador),2,'0') || xMMSS AS TIME(0)) AS TIME(6));
                       SET xSchdMatrixCD = xSchdMatrixCD +1;      
                       SET iContador = iContador + 1;
                    ELSE 
                       SET iContador = 24;
                    END IF;
                    
                   END;
                    
                END WHILE;
             
             ELSEIF Reg.UnidadFrecuenciaCd = 104 THEN -- Semanal
                    
                SELECT Cast(day_of_week AS SMALLINT)
                  INTO xDiaSemana
                  FROM SYS_CALENDAR.CALENDAR
                 WHERE Calendar_Date = Current_Date;

                -- Agregado por GQC 20210106 ****************************************
                SET v_ValorFrecuenciaAdic = Reg.ValorFrecuenciaAdic; 
          
                SELECT Instr(v_ValorFrecuenciaAdic , Cast(xDiaSemana AS VARCHAR(2))) INTO xFlagBusqueda;
                                
                --IF xDiaSemana = Reg.ValorFrecuencia THEN 
                IF xDiaSemana = Reg.ValorFrecuencia OR xFlagBusqueda > 0 THEN 
                -- Fin cambios    20210106 ******************************************
                   
                   SET xScheduleCD = Reg.ScheduleCD;
                   SET xSchdMatrixCDPred = Reg.ScheduleCdPre;
                   SET xHorIni = Reg.HoraInicio;
                   SET xHorMaxEjec = Reg.HoraFinalizacion;
                   SET xFecCreaTS = Cast(Current_Timestamp AS TIMESTAMP(6));

                   SET V_SQLText = 'INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ VALUES ( ' ||
                   Cast(xSchdMatrixCD AS VARCHAR(10)) || ',' || Cast(xScheduleCD AS VARCHAR(10)) || ',' || Cast(xSchdMatrixCDPred AS VARCHAR(20)) || ',''' ||
                   Coalesce(To_Char(xFecIniEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || Coalesce(To_Char(xFecFinEjec_TS,'YYYY-MM-DD HH24:MI:SSFF'),'NULL') || ''',''' || To_Char(xFecIni,'YYYY-MM-DD') || ''',''' || Cast(xHorIni AS VARCHAR(15)) || ''',''' ||
                   Cast(xHorMaxEjec AS VARCHAR(15)) || ''',''' || To_Char(xFecCreaTS,'YYYY-MM-DD HH24:MI:SSFF') || ''',' || Cast(xEstadoCD AS VARCHAR(4)) || ',' || Coalesce(Cast(xNumEjec AS VARCHAR(4)),'NULL') || ');';
                 
                   INSERT INTO PE_DESA_FG_CONFIG.LOGS_SPS_DETAILS
                   VALUES (V_SP, iContador, Current_Timestamp, V_SQLText);

                   INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ
                   VALUES ( :xSchdMatrixCD, :xScheduleCD, :xSchdMatrixCDPred,
                            :xFecIniEjec_TS, :xFecFinEjec_TS, :xFecIni,
                            :xHorIni, :xHorMaxEjec, :xFecCreaTS, :xEstadoCD, :xNumEjec );
                                    
                   SET iContador = iContador + 1;
                           
                END IF;
                    
             END IF;
             
        END IF;
        
        SET xSchdMatrixCD = xSchdMatrixCD + 1;
    END FOR;
	
    IF (v_loop = 0) THEN
       SET v_sw = 1 ;-- Sale del loop
    END IF;
    
 END WHILE;

 -- INSERTAMOS LOS PENDIENTES DEL DIA - 1
 INSERT INTO PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ
 SELECT *
   FROM PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ_HIST
  WHERE EstadoCD <> 53
	AND HorIni > horMaxEjec
	AND FecIni = DATE - 1;

  -- Conteo de Nro Registros de Antes de Ejecucion
  SELECT Coalesce(Count(1),0) + 1
    INTO xRegsDespues
    FROM PE_DESA_FG_CONFIG.TB_SCHEDULE_MATRIZ t;
    
 SET qryUpdLog =
 'UPDATE ' || V_BDGestorConf || '.' || V_TablaLog ||
 ' SET Fecha_Ejecucion_Fin = CURRENT_DATE, Hora_Ejecucion_Fin = CURRENT_TIME(0), Estado_Ejecucion = ''53'', Descripcion_Error = ''Procesado correctamente''' ||
 ', Nro_Registros_Despues = ' || Cast(xRegsDespues AS VARCHAR(10)) || ' WHERE Id_Agendamiento = ' || V_Id_Agendamiento || ';';
 CALL DBC.SysExecSQL(qryUpdLog);
    
END;