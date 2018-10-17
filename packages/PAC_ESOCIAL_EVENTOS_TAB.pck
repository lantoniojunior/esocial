create or replace package PAC_ESOCIAL_EVENTOS_TAB is

-- Author  : LUCAS
-- Created : 15/10/2018 14:26:25
-- Purpose : 

end PAC_ESOCIAL_EVENTOS_TAB;
/
CREATE OR REPLACE PACKAGE BODY PAC_ESOCIAL_EVENTOS_TAB IS

  GB_REC_ERRO        ESOCIAL.TSOC_CTR_ERRO_PROCESSO%ROWTYPE;
  GB_ID_CTR_PROCESSO ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE;
  GB_ID_ORIGEM       ESOCIAL.TSOC_PAR_ORIGEM.ID_ORIGEM%TYPE;
  GB_ID_EVENTO       ESOCIAL.TSOC_PAR_EVENTO.ID_EVENTO%TYPE;
  GB_ID_PERIODO_DET  ESOCIAL.TSOC_CTR_PERIODO_DET.ID_PERIODO_DET%TYPE;
  GB_COD_INS         NUMBER;
  GB_DAT_EVT_ATU VARCHAR2(100);
  GB_DAT_EVT_ANT VARCHAR2(100);
  GB_SEQ_CHAVE_ID NUMBER;

  TYPE GB_TY_EMPREGADOR IS RECORD(
    TP_INSC  ESOCIAL.TSOC_CAD_EMPREGADOR.TP_INSC%TYPE,
    NUM_CNPJ ESOCIAL.TSOC_CAD_EMPREGADOR.NUM_CNPJ%TYPE);

  GB_EMPREGADOR GB_TY_EMPREGADOR;

  CURSOR C_1010_INI IS(
    SELECT DISTINCT COD_RUBRICA, NOM_RUBRICA
      FROM USER_IPESP.TB_RUBRICAS RB
     WHERE NOT EXISTS (SELECT 1
              FROM TSOC_1010_RUBRICA T
             WHERE T.IDERUBRICA_CODRUBR = RB.COD_RUBRICA
               AND T.COD_INS = GB_COD_INS));

  PROCEDURE SP_DEFAULT_SESSION IS
  BEGIN
  
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_LANGUAGE =  ''BRAZILIAN PORTUGUESE''';
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TERRITORY = ''BRAZIL''';
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_LANGUAGE = ''BRAZILIAN PORTUGUESE''';
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''DD/MM/YYYY''';
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,''';
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_CURRENCY = ''R$''';
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_CALENDAR = ''GREGORIAN''';
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_ISO_CURRENCY  = ''BRAZIL''';
  
  END SP_DEFAULT_SESSION;

  PROCEDURE SP_SETA_PROCESSO(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE,
                             P_NOM_EVENTO      IN VARCHAR2) IS
  BEGIN
  
    IF P_NOM_EVENTO = 'INICIO_PROCESSAMENTO' THEN
    
      UPDATE ESOCIAL.TSOC_CTR_PROCESSO
         SET DAT_INICIO      = SYSDATE,
             DAT_FIM         = NULL,
             FLG_STATUS      = 'P',
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_SETA_PROCESSO'
       WHERE ID_CTR_PROCESSO = P_ID_CTR_PROCESSO;
    
      COMMIT;
    
    ELSIF P_NOM_EVENTO = 'FIM_PROCESSAMENTO' THEN
    
      UPDATE ESOCIAL.TSOC_CTR_PROCESSO
         SET DAT_FIM         = SYSDATE,
             FLG_STATUS      = 'F',
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_SETA_PROCESSO'
       WHERE ID_CTR_PROCESSO = P_ID_CTR_PROCESSO;
    
      COMMIT;
    
    ELSIF P_NOM_EVENTO = 'ATUALIZA_QUANTIDADE' THEN
    
      --ATUALIZACAO DE QUANTIDADE DE REGISTROS
      UPDATE ESOCIAL.TSOC_CTR_PROCESSO
         SET QTD_REGISTROS   = NVL(QTD_REGISTROS, 0) + 1,
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_SETA_PROCESSO'
       WHERE ID_CTR_PROCESSO = P_ID_CTR_PROCESSO;
    
      COMMIT;
    
    ELSE
    
      --ERRO NO PROCESSAMENTO
      UPDATE ESOCIAL.TSOC_CTR_PROCESSO
         SET FLG_STATUS      = 'E',
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_SETA_PROCESSO'
       WHERE ID_CTR_PROCESSO = P_ID_CTR_PROCESSO;
    
      COMMIT;
    
    END IF;
  
  END SP_SETA_PROCESSO;

  PROCEDURE SP_GERA_ERRO_PROCESSO IS
    V_ID_CAD_ERRO ESOCIAL.TSOC_CTR_ERRO_PROCESSO.ID_ERRO%TYPE;
  BEGIN
  
    V_ID_CAD_ERRO := ESOCIAL.ESOC_SEQ_ID_ERRO_PROCESSO.NEXTVAL;
  
    INSERT INTO ESOCIAL.TSOC_CTR_ERRO_PROCESSO
      (ID_ERRO,
       COD_INS,
       ID_CAD,
       NOM_PROCESSO,
       TIPO_EVENTO,
       DESC_ERRO,
       DAT_ING,
       DAT_ULT_ATU,
       NOM_USU_ULT_ATU,
       NOM_PRO_ULT_ATU,
       DESC_ERRO_BD,
       NUM_PROCESSO,
       DES_IDENTIFICADOR,
       FLG_TIPO_ERRO,
       ID_CTR_PROCESSO)
    VALUES
      (V_ID_CAD_ERRO,
       GB_REC_ERRO.COD_INS,
       GB_REC_ERRO.ID_CAD,
       GB_REC_ERRO.NOM_PROCESSO,
       GB_REC_ERRO.TIPO_EVENTO,
       GB_REC_ERRO.DESC_ERRO,
       SYSDATE,
       SYSDATE,
       'ESOCIAL',
       'SP_GERA_ERRO_PROCESSO',
       GB_REC_ERRO.DESC_ERRO_BD,
       GB_REC_ERRO.NUM_PROCESSO,
       GB_REC_ERRO.DES_IDENTIFICADOR,
       GB_REC_ERRO.FLG_TIPO_ERRO,
       GB_ID_CTR_PROCESSO);
  
    COMMIT;
  
  END SP_GERA_ERRO_PROCESSO;

  PROCEDURE SP_CARREGA_IDS(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE) IS
  BEGIN
  
    SELECT B.ID_ORIGEM, B.ID_EVENTO, C.ID_PERIODO_DET, C.COD_INS
      INTO GB_ID_ORIGEM, GB_ID_EVENTO, GB_ID_PERIODO_DET, GB_COD_INS
    
      FROM ESOCIAL.TSOC_CTR_PROCESSO    A,
           ESOCIAL.TSOC_PAR_PROCESSO    B,
           ESOCIAL.TSOC_CTR_PERIODO_DET C,
           ESOCIAL.TSOC_CTR_PERIODO     D
     WHERE A.COD_INS = B.COD_INS
       AND A.ID_PROCESSO = B.ID_PROCESSO
       AND A.COD_INS = C.COD_INS
       AND A.ID_PERIODO = C.ID_PERIODO
       AND B.COD_INS = C.COD_INS
       AND B.ID_EVENTO = C.ID_EVENTO
       AND A.ID_CTR_PROCESSO = P_ID_CTR_PROCESSO
       AND D.ID_PERIODO = C.ID_PERIODO
       AND D.COD_INS = C.COD_INS
       AND B.FLG_STATUS = 'A' --PROCESSO COM STATUS ATIVO
       AND A.FLG_STATUS = 'A' --COM STATUS AGENDADO 
       AND C.FLG_STATUS IN ('A', 'R') --PER�ODO ABERTO OU REABERTO PARA O EVENTO
       AND D.FLG_STATUS IN ('A', 'R'); --PERIODO ABERTO OU REABERTO
  END SP_CARREGA_IDS;

  PROCEDURE SP_RET_INSC_EMP IS
  BEGIN
    SELECT EMP.NUM_CNPJ, EMP.TP_INSC
      INTO GB_EMPREGADOR.NUM_CNPJ, GB_EMPREGADOR.TP_INSC
      FROM ESOCIAL.TSOC_CAD_EMPREGADOR  EMP,
           ESOCIAL.TSOC_CTR_PERIODO_DET PD,
           ESOCIAL.TSOC_PAR_ORIGEM      PO
     WHERE EMP.COD_INS = GB_COD_INS
       AND PD.ID_ORIGEM = PO.ID_ORIGEM
       AND PO.ID_CAD_EMPREGADOR = EMP.ID_CAD_EMPREGADOR
       AND PD.COD_INS = EMP.COD_INS
       AND PD.ID_PERIODO_DET = GB_ID_PERIODO_DET
       AND PD.ID_ORIGEM = GB_ID_ORIGEM;
  
  EXCEPTION
    WHEN OTHERS THEN
      GB_EMPREGADOR.TP_INSC  := NULL;
      GB_EMPREGADOR.NUM_CNPJ := NULL;
    
  END SP_RET_INSC_EMP;
  
  
  FUNCTION FC_GERA_ID_EVENTO RETURN VARCHAR2 IS
  BEGIN
  
    RETURN 'ID' || GB_EMPREGADOR.TP_INSC || GB_EMPREGADOR.NUM_CNPJ || GB_DAT_EVT_ATU || LPAD(GB_SEQ_CHAVE_ID,
                                                                                             5,
                                                                                             0);
  END FC_GERA_ID_EVENTO;
 

  PROCEDURE SP_1010_RUBRICA(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE) IS
    EX_PARAM_PROC EXCEPTION;
    V_1010_INI ESOCIAL.TSOC_1010_RUBRICA%ROWTYPE;
  BEGIN
  
    SP_DEFAULT_SESSION;
    SP_SETA_PROCESSO(P_ID_CTR_PROCESSO, 'INICIO_PROCESSAMENTO');
    GB_ID_CTR_PROCESSO := P_ID_CTR_PROCESSO;
  
    BEGIN
      SP_CARREGA_IDS(P_ID_CTR_PROCESSO);
    EXCEPTION
      WHEN OTHERS THEN
        RAISE EX_PARAM_PROC;
    END;
  
    V_1010_INI.COD_INS        := GB_COD_INS;
    V_1010_INI.ID_PERIODO_DET := GB_ID_PERIODO_DET;
    --OBT�M N�MERO DE INSCRI��O E TIPO DE INSCRI��O DO EMPREGADOR 
    SP_RET_INSC_EMP;
    V_1010_INI.TPINSC := GB_EMPREGADOR.TP_INSC;
    V_1010_INI.NRINSC := GB_EMPREGADOR.NUM_CNPJ;
  
    OPEN C_1010_INI;    
     LOOP 
       
     GB_DAT_EVT_ANT := GB_DAT_EVT_ATU;
     GB_DAT_EVT_ATU := TO_CHAR(SYSDATE, 'YYYYMMDDHH24MMSS');   
     
     
       FETCH C_1010_INI
         INTO V_1010_INI.IDERUBRICA_CODRUBR, 
              V_1010_INI.DADOSRUBRICA_DSCRUBR;
              
     IF GB_DAT_EVT_ATU = GB_DAT_EVT_ANT THEN
        GB_SEQ_CHAVE_ID := GB_SEQ_CHAVE_ID + 1;
      ELSE
        GB_SEQ_CHAVE_ID := 1;
      END IF; 
      
      V_1010_INI.ID_PK := FC_GERA_ID_EVENTO;
      
              
              
       
       
                
    
    
    END LOOP; 
    
   /* ID_PK
    COD_INS
    ID
    TPAMB
    PROCEMI
    VERPROC
    TPINSC
    NRINSC
    IDERUBRICA_CODRUBR
    IDERUBRICA_IDETABRUBR
    IDERUBRICA_INIVALID
    IDERUBRICA_FIMVALID
    DADOSRUBRICA_DSCRUBR
    DADOSRUBRICA_NATRUBR
    DADOSRUBRICA_TPRUBR
    DADOSRUBRICA_CODINCCP
    DADOSRUBRICA_CODINCIRRF
    DADOSRUBRICA_CODINCFGTS
    DADOSRUBRICA_CODINCSIND
    DADOSRUBRICA_OBSERVACAO
    IDEPROCESSOCP_TPPROC
    IDEPROCESSOCP_NRPROC
    IDEPROCESSOCP_EXTDECISAO
    IDEPROCESSOCP_CODSUSP
    IDEPROCESSOIRRF_NRPROC
    IDEPROCESSOIRRF_CODSUSP
    IDEPROCESSOFGTS_NRPROC
    IDEPROCESSOSIND_NRPROC
    NOVAVALIDADE_INIVALID
    NOVAVALIDADE_FIMVALID
    ID_ORIGEM
    ID_LOTE
    CTR_FLG_STATUS
    DAT_ING
    DAT_ULT_ATU
    NOM_USU_ULT_ATU
    NOM_PRO_ULT_ATU
    XML_ENVIO
    FLG_VIGENCIA
    CTR_DSC_EVENTO
    ID_PERIODO_DET
    DADOSRUBRICA_CODINCCPRP
    DADOSRUBRICA_TETOREMUN
    IDEPROCESSOCPRP_TPPROC
    IDEPROCESSOCPRP_NRPROC
    IDEPROCESSOCPRP_EXTDECISAO*/

  
   
   
    --SP_INC_1010(V_1010_INI); 
  
  EXCEPTION
    WHEN EX_PARAM_PROC THEN
      GB_REC_ERRO.COD_INS           := GB_COD_INS;
      GB_REC_ERRO.ID_CAD            := NULL;
      GB_REC_ERRO.NOM_PROCESSO      := 'SP_1010_RUBRICA';
      GB_REC_ERRO.TIPO_EVENTO       := '1010';
      GB_REC_ERRO.DESC_ERRO         := 'ERRO NA PARAMETRIZA��O DO PROCESSO';
      GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
      GB_REC_ERRO.DES_IDENTIFICADOR := NULL;
      GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
      SP_GERA_ERRO_PROCESSO;
      SP_SETA_PROCESSO(P_ID_CTR_PROCESSO, 'ERRO_PROCESSAMENTO');
    
    WHEN OTHERS THEN
      GB_REC_ERRO.COD_INS           := GB_COD_INS;
      GB_REC_ERRO.ID_CAD            := NULL;
      GB_REC_ERRO.NOM_PROCESSO      := 'SP_1010_RUBRICA';
      GB_REC_ERRO.TIPO_EVENTO       := '1010';
      GB_REC_ERRO.DESC_ERRO         := 'ERRO NO PROCESSO DE GERA��O DE EVENTO DE RUBRICA';
      GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
      GB_REC_ERRO.DES_IDENTIFICADOR := NULL;
      GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
      SP_GERA_ERRO_PROCESSO;
      SP_SETA_PROCESSO(P_ID_CTR_PROCESSO, 'ERRO_PROCESSAMENTO');
    
  END SP_1010_RUBRICA;

END PAC_ESOCIAL_EVENTOS_TAB;
/