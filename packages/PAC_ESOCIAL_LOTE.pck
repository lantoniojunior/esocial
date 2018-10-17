CREATE OR REPLACE PACKAGE PAC_ESOCIAL_LOTE IS

  -- AUTHOR  : LUCAS
  -- CREATED : 02/10/2018 14:31:59
  -- PURPOSE : GERA LOTE DE EVENTOS

 --ERROS DO PROCESSO
  GB_REC_ERRO ESOCIAL.TSOC_CTR_ERRO_PROCESSO%ROWTYPE;

  --FUNCTION FC_ASSINA_XML(P_XML IN CLOB) RETURN CLOB;
  

  PROCEDURE SP_ATUALIZA_EVENTO(P_ID_PK        IN NUMBER,
                               P_TABELA       IN VARCHAR2,
                               P_ID_LOTE      IN NUMBER,
                               P_COD_INS      IN NUMBER,
                               P_TIP_ATU IN VARCHAR2 --EA, AL
                               );
  PROCEDURE SP_GERA_LOTE(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE);
  
  --REGISTRA DATA FIM DE PROCESSAMENTO, QUANTIDADE DE LINHAS PROCESSADAS
  --E STATUS DO PROCESSAMENTO NA TB_CTR_PROCESSO 
  PROCEDURE SP_SETA_PROCESSO(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE,
                             P_NOM_EVENTO    IN VARCHAR2);
  
  
  --GERA O ERRO NA TABELA DE LOG DE ERRO DE PROCESSOS
  PROCEDURE SP_GERA_LOG_ERRO(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE);
  
END;
/
CREATE OR REPLACE PACKAGE BODY PAC_ESOCIAL_LOTE IS

  TYPE T_LOTE IS REF CURSOR;

  TYPE GB_TY_ID IS RECORD(
    ID_ORIGEM      ESOCIAL.TSOC_PAR_ORIGEM.ID_ORIGEM%TYPE,
    ID_EVENTO      ESOCIAL.TSOC_PAR_EVENTO.ID_EVENTO%TYPE,
    ID_PERIODO_DET ESOCIAL.TSOC_CTR_PERIODO_DET.ID_PERIODO_DET%TYPE,
    COD_INS        ESOCIAL.TSOC_CTR_PROCESSO.COD_INS%TYPE);

  GB_REC_ID GB_TY_ID;

  PROCEDURE SP_CARREGA_IDS(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE) IS
    
  BEGIN

  
  
    SELECT B.ID_ORIGEM, B.ID_EVENTO, C.ID_PERIODO_DET, C.COD_INS
      INTO GB_REC_ID.ID_ORIGEM, 
           GB_REC_ID.ID_EVENTO, 
           GB_REC_ID.ID_PERIODO_DET, 
           GB_REC_ID.COD_INS
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
       AND B.FLG_STATUS = 'A' --PROCESSO PARAMETRIZADO COMO ATIVO
       AND A.FLG_STATUS = 'A' --PROCESSO COM STATUS P DEFINIDO NO INICIO DO PROGRAMA
       AND C.FLG_STATUS IN ('A', 'R') --PERÍODO ABERTO OU REABERTO PARA O EVENTO
       AND D.FLG_STATUS IN ('A', 'R'); --PERIODO ABERTO OU REABERTO
  
       
  END SP_CARREGA_IDS;


--Obtém Capacidade de eventos por lote.   
FUNCTION FC_RET_CAP_LOTE RETURN NUMBER
 IS 
V_CAP_LOTE NUMBER; 
BEGIN 
       SELECT L.QTD_MAX_EVENTOS 
        INTO V_CAP_LOTE 
        FROM ESOCIAL.TSOC_PAR_LOTE L     
      WHERE (L.DAT_FIM_VIG IS NULL OR L.DAT_FIM_VIG >= SYSDATE)
        AND L.DAT_INI_VIG <= SYSDATE; 
        
        RETURN V_CAP_LOTE;
        
  
END FC_RET_CAP_LOTE; 

                  

  --MONTA O CURSOR DE ACORDO COM O AGENDAMENTO PARA EVENTOS COM STATUS 'AL'

  PROCEDURE SP_DY_CURSOR(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE,
                         P_LOTE            IN OUT NOCOPY T_LOTE,
                         P_TABELA          OUT NOCOPY VARCHAR2,
                         P_COD_TIPO_EVENTO OUT NOCOPY NUMBER,
                         P_MAX_ID_PK       OUT NOCOPY NUMBER
                         ) IS
  
    V_SQL_STMT     VARCHAR2(4000);
    V_SQL_STMT2    VARCHAR2(4000);
    V_MIN_ID_PK    NUMBER(16);
    V_MAX_ID_PK    NUMBER(16);
    V_CTR_PROCESSO ESOCIAL.TSOC_CTR_PROCESSO%ROWTYPE;
  BEGIN
  
    --OBTÉM INFORMAÇÕES DO AGENDAMENTO   
    SELECT *
      INTO V_CTR_PROCESSO
      FROM ESOCIAL.TSOC_CTR_PROCESSO CP
     WHERE CP.ID_CTR_PROCESSO = P_ID_CTR_PROCESSO;
  
    --OBTÉM A TABELA DE EVENTOS. 
    SELECT E.NOM_TABELA, E.COD_EVENTO
      INTO P_TABELA, P_COD_TIPO_EVENTO
      FROM ESOCIAL.TSOC_PAR_EVENTO E
     WHERE E.COD_INS = GB_REC_ID.COD_INS
       AND E.ID_EVENTO = GB_REC_ID.ID_EVENTO;
  
    --Caso não possua paralelismo, considerar do menor ao maior valor
    --de ID_PK disponível para processamento       
    IF V_CTR_PROCESSO.FAIXA_INI IS NULL OR V_CTR_PROCESSO.FAIXA_FIM IS NULL THEN
      V_SQL_STMT2 := 'SELECT MIN(ID_PK), MAX(ID_PK) FROM ' || P_TABELA ||
                     ' WHERE CTR_FLG_STATUS = ''AL''';
      EXECUTE IMMEDIATE V_SQL_STMT2
        INTO V_MIN_ID_PK, V_MAX_ID_PK;
    ELSE
      V_MIN_ID_PK := V_CTR_PROCESSO.FAIXA_INI;
      V_MAX_ID_PK := V_CTR_PROCESSO.FAIXA_FIM;
    END IF;
    
    P_MAX_ID_PK := V_MAX_ID_PK; 
  
    --MONTA O CURSOR PARA LOTE
  
    V_SQL_STMT := 'SELECT ID_PK,XML_ENVIO,COD_INS,ID FROM ' ||
                  P_TABELA || ' WHERE CTR_FLG_STATUS = ''AL''
                AND ID_PK >= ' || V_MIN_ID_PK ||
                  ' AND ID_PK <= ' || V_MAX_ID_PK;
    OPEN P_LOTE FOR V_SQL_STMT;
  
  END SP_DY_CURSOR;

  PROCEDURE SP_GERA_LOTE(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE) IS
  
    V_LOTE             T_LOTE;
    V_XML_LOTE         CLOB;
    V_QTD_EVT_LOTE     NUMBER := 0; 
    V_XML_EVENTO       CLOB; 
    V_TABELA           VARCHAR2(100);
    V_ID_PK            NUMBER(16);
    V_ID               VARCHAR2(36); --Id Gerado do evento no XML 
    V_MAX_ID_PK        NUMBER; 
    V_COD_INS          NUMBER;
    V_COD_TIP_EVENTO   ESOCIAL.TSOC_PAR_EVENTO.COD_EVENTO%TYPE;
    V_ID_LOTE          ESOCIAL.TSOC_CTR_LOTE.ID_LOTE%TYPE;
    EX_PARAM_PROC EXCEPTION;
    V_CAP_LOTE NUMBER; --Tamanho limite de eventos por lote 
    V_HEADER CLOB; --Cabeçalho do xml do Lote
    V_TRAILER CLOB; --Rodapé do XML do Lote 
        
  
  BEGIN

    --Obtém Ids
    BEGIN
      SP_CARREGA_IDS(P_ID_CTR_PROCESSO);
    EXCEPTION
      WHEN OTHERS THEN
        RAISE EX_PARAM_PROC;
    END;
    
     SP_SETA_PROCESSO(P_ID_CTR_PROCESSO, 'INICIO_PROCESSAMENTO');
    
     --Obtém capacidade do lote de acordo com a parametrização 
     V_CAP_LOTE := FC_RET_CAP_LOTE;  
        
    --Monta o cursor
     SP_DY_CURSOR(P_ID_CTR_PROCESSO, V_LOTE, V_TABELA, V_COD_TIP_EVENTO, V_MAX_ID_PK);
  
    LOOP
    
      BEGIN
        
        FETCH V_LOTE
          INTO V_ID_PK, V_XML_EVENTO, V_COD_INS, V_ID;
          

        --1. Abre o lote
        --2. Associa Eventos ao Lote
        --3. Concatena o XML dos eventos para formar o XML do lote
        --4. Quando chegar em 50 eventos (Capacidade do lote parametrizada), Grava o XML do Lote.
        --5. Gera um novo lote após 50 eventos. 
        --5. Se não houver mais eventos, grava o XML do lote atual

      
        --Abre o Lote    
        IF MOD(V_LOTE%ROWCOUNT, V_CAP_LOTE) = 1 THEN
          V_ID_LOTE := ESOCIAL.ESOC_SEQ_ID_LOTE.NEXTVAL;
        
         --Obtém o cabeçalho o rodapé do lote. 
            PAC_ESOCIAL_XML.SP_XML_LOTE(GB_REC_ID.ID_EVENTO,V_MAX_ID_PK,V_HEADER,V_TRAILER);
        
        
          V_XML_LOTE := V_HEADER || CHR(13); 
        
          INSERT INTO ESOCIAL.TSOC_CTR_LOTE
            (ID_LOTE,
             ID_PERIODO_DET,
             ID_ORIGEM,
             ID_EVENTO,
             COD_INS,
             FLG_STATUS,
             CTR_IDE_PROTOCOLO_ENVIO,
             CTR_DAT_ENVIO,
             CTR_IDE_RETORNO,
             CTR_DAT_RETORNO,
             QTD_EVENTO,
             DATA_ESTIMADA_CONSULTA,
             XML_LOTE,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU,
             ID_CTR_PROCESSO)
          VALUES
            (V_ID_LOTE,
             GB_REC_ID.ID_PERIODO_DET,
             GB_REC_ID.ID_ORIGEM,
             GB_REC_ID.ID_EVENTO,
             GB_REC_ID.COD_INS,
             'A',
             NULL,
             NULL,
             NULL,
             NULL, --QTD_EVENTOS TRATAR
             NULL,
             NULL, --DAT_ESTIMADA
             NULL, --XML DO LOTE
             SYSDATE,
             SYSDATE,
             'ESOCIAL',
             'SP_GERA_LOTE',
             P_ID_CTR_PROCESSO);
        
          COMMIT;
        
        END IF;    
      
        --Associa Eventos ao Lote
        SP_ATUALIZA_EVENTO(V_ID_PK, V_TABELA, V_ID_LOTE, V_COD_INS, 'AE');
      
        --Obtém o XML do Evento                   
        V_XML_LOTE := V_XML_LOTE || '<evento Id="'|| V_ID || '">' || CHR(13) || V_XML_EVENTO || CHR(13) || '</evento>' || CHR(13);
        
        --Contabiliza o evento dentro do lote
         V_QTD_EVT_LOTE := V_QTD_EVT_LOTE+1; 
        
      
        --Se completou 50 eventos, grava o XML do Lote  
        --E grava a quantidade de eventos que nele contém 
        IF MOD(V_LOTE%ROWCOUNT, V_CAP_LOTE) = 0 THEN      
          
         V_XML_LOTE := V_XML_LOTE || CHR(13) || V_TRAILER; 
          
          UPDATE ESOCIAL.TSOC_CTR_LOTE L
             SET L.XML_LOTE        = V_XML_LOTE,
                 L.QTD_EVENTO      = V_QTD_EVT_LOTE,
                 L.DAT_ING         = SYSDATE,
                 L.DAT_ULT_ATU     = SYSDATE,
                 L.NOM_USU_ULT_ATU = 'ESOCIAL',
                 L.NOM_PRO_ULT_ATU = 'SP_GERA_LOTE'
           WHERE L.ID_LOTE = V_ID_LOTE;                    
                     
          V_QTD_EVT_LOTE := 0;
                
        --Se for o último evento, grava o XML com os eventos restantes
      ELSIF V_LOTE%ROWCOUNT = V_MAX_ID_PK THEN                 
          --Associa Eventos ao Lote              
          UPDATE ESOCIAL.TSOC_CTR_LOTE L
             SET L.XML_LOTE        = V_XML_LOTE,
                 L.QTD_EVENTO      = V_QTD_EVT_LOTE,
                 L.DAT_ING         = SYSDATE,
                 L.DAT_ULT_ATU     = SYSDATE,
                 L.NOM_USU_ULT_ATU = 'ESOCIAL',
                 L.NOM_PRO_ULT_ATU = 'SP_GERA_LOTE'
           WHERE L.ID_LOTE = V_ID_LOTE;
            
 
        
          COMMIT;
          EXIT;
          
        END IF;
      
      EXCEPTION
        WHEN OTHERS THEN
          GB_REC_ERRO.COD_INS           := V_COD_INS;
          GB_REC_ERRO.ID_CAD            := V_ID_PK;
          GB_REC_ERRO.NOM_PROCESSO      := 'SP_GERA_LOTE';
          GB_REC_ERRO.TIPO_EVENTO       := V_COD_TIP_EVENTO;
          GB_REC_ERRO.DESC_ERRO         := 'ERRO NA GERAÇÃO DO LOTE';
          GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
          GB_REC_ERRO.DES_IDENTIFICADOR := V_TABELA;
          GB_REC_ERRO.FLG_TIPO_ERRO     := 'E';
          SP_GERA_LOG_ERRO(P_ID_CTR_PROCESSO);
          --Marca status EA (Erro de Geração de Lote) no evento. 
          SP_ATUALIZA_EVENTO(V_ID_PK, V_TABELA, V_ID_LOTE, V_COD_INS, 'EL');
      END;
    
      SP_SETA_PROCESSO(P_ID_CTR_PROCESSO, 'ATUALIZA_QUANTIDADE');
         
    END LOOP;
  
    SP_SETA_PROCESSO(P_ID_CTR_PROCESSO, 'FIM_PROCESSAMENTO');
  
  EXCEPTION
    WHEN EX_PARAM_PROC THEN
      GB_REC_ERRO.COD_INS           := V_COD_INS;
      GB_REC_ERRO.ID_CAD            := V_ID_PK;
      GB_REC_ERRO.NOM_PROCESSO      := 'SP_CARREGA_IDS';
      GB_REC_ERRO.TIPO_EVENTO       := V_COD_TIP_EVENTO;
      GB_REC_ERRO.DESC_ERRO         := 'ERRO NA PARAMETRIZAÇÃO DO PROCESSO';
      GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
      GB_REC_ERRO.DES_IDENTIFICADOR := V_TABELA;
      GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
      SP_GERA_LOG_ERRO(P_ID_CTR_PROCESSO);
      SP_SETA_PROCESSO(P_ID_CTR_PROCESSO, 'ERRO');
    
    WHEN OTHERS THEN
      GB_REC_ERRO.COD_INS           := V_COD_INS;
      GB_REC_ERRO.ID_CAD            := V_ID_PK;
      GB_REC_ERRO.NOM_PROCESSO      := 'SP_DY_CURSOR';
      GB_REC_ERRO.TIPO_EVENTO       := V_COD_TIP_EVENTO;
      GB_REC_ERRO.DESC_ERRO         := 'ERRO NO PROCESSO DE LOTE';
      GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
      GB_REC_ERRO.DES_IDENTIFICADOR := V_TABELA;
      GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
      SP_GERA_LOG_ERRO(P_ID_CTR_PROCESSO);
      SP_SETA_PROCESSO(P_ID_CTR_PROCESSO, 'ERRO');
    
  END SP_GERA_LOTE;

  PROCEDURE SP_ATUALIZA_EVENTO(P_ID_PK   IN NUMBER,
                               P_TABELA  IN VARCHAR2,
                               P_ID_LOTE IN NUMBER,
                               P_COD_INS IN NUMBER,
                               P_TIP_ATU IN VARCHAR2 --EA, AL
                               ) IS
    V_UPDATE_STMT VARCHAR2(4000);
  
  BEGIN
  
    IF P_TIP_ATU = 'AE' THEN
    
      V_UPDATE_STMT := 'UPDATE ' || P_TABELA ||
                       '  SET CTR_FLG_STATUS = ''AE'', 
                       ID_LOTE = ' || P_ID_LOTE || ',
                       DAT_ING = ' || CHR(39) ||
                       SYSDATE || CHR(39) || ', 
                       DAT_ULT_ATU = ' || CHR(39) ||
                       SYSDATE || CHR(39) || ', 
                       NOM_USU_ULT_ATU = ''ESOCIAL'', 
                       NOM_PRO_ULT_ATU = ''SP_ATUALIZA_EVENTO''
                      WHERE ID_PK = ' || P_ID_PK ||
                       ' AND COD_INS = ' || P_COD_INS;
    
    ELSE
    
      V_UPDATE_STMT := 'UPDATE ' || P_TABELA ||
                       ' SET 
                        CTR_FLG_STATUS = ''EL'', 
                        DAT_ING = ' || CHR(39) ||
                       SYSDATE || CHR(39) || ', 
                        DAT_ULT_ATU = ' || CHR(39) ||
                       SYSDATE || CHR(39) || ', 
                        NOM_USU_ULT_ATU = ''ESOCIAL'', 
                        NOM_PRO_ULT_ATU = ''SP_ATUALIZA_EVENTO'' ' ||
                       ' WHERE ID_PK = ' || P_ID_PK || 'AND COD_INS = ' ||
                       P_COD_INS;
    
    END IF;
  
    EXECUTE IMMEDIATE V_UPDATE_STMT;
    COMMIT;
  
  END SP_ATUALIZA_EVENTO;

  PROCEDURE SP_GERA_LOG_ERRO(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE) IS
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
       P_ID_CTR_PROCESSO);
  
    COMMIT;
  END SP_GERA_LOG_ERRO;
      

  --SETA O STATUS DE PROCESSAMENTO E QUANTIDADE DE REGISTROS DA TSOC_CTR_PROCESSO
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

END PAC_ESOCIAL_LOTE;
/
