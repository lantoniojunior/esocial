CREATE OR REPLACE PACKAGE PAC_ESOCIAL_ASSINATURA IS

  -- Author  : LUCAS
  -- Created : 10/09/2018 14:31:59
  -- Purpose : Assina os eventos xml por meio de uma chamada de webservice

  --Erros do processo
  GB_REC_ERRO ESOCIAL.TSOC_CTR_ERRO_PROCESSO%ROWTYPE;

  FUNCTION FC_ASSINA_XML(P_XML IN CLOB) RETURN CLOB;

  PROCEDURE SP_ATUALIZA_EVENTO(P_ID_PK        IN NUMBER,
                               P_TABELA       IN VARCHAR2,
                               P_XML_ASSINADO IN CLOB,
                               P_COD_INS      IN NUMBER,
                               P_TIP_ATU      IN VARCHAR2 --EA, AL
                               );
  PROCEDURE SP_GERA_ASSINATURA(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE);

  --Registra data fim de processamento, quantidade de linhas processadas
  --e status do processamento na TB_CTR_PROCESSO 
  PROCEDURE SP_SETA_PROCESSO(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE,
                             P_NOM_EVENTO      IN VARCHAR2);

  --Gera o erro na tabela de log de erro de processos
  PROCEDURE SP_GERA_LOG_ERRO(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE);

end PAC_ESOCIAL_ASSINATURA;
/
CREATE OR REPLACE PACKAGE BODY PAC_ESOCIAL_ASSINATURA IS

  TYPE T_ASSINATURA IS REF CURSOR;

  FUNCTION FC_ASSINA_XML(P_XML IN CLOB) RETURN CLOB AS
    L_REQUEST  SOAP_API.T_REQUEST;
    L_RESPONSE SOAP_API.T_RESPONSE;
  
    L_RETURN       CLOB;
    L_PARAM_ENCODE VARCHAR2(32767);
    L_URL          VARCHAR2(32767);
    L_NAMESPACE    VARCHAR2(32767);
    L_METHOD       VARCHAR2(32767);
    L_SOAP_ACTION  VARCHAR2(32767);
    L_RESULT_NAME  VARCHAR2(32767);
  BEGIN
    L_PARAM_ENCODE := DBMS_XMLGEN.CONVERT(P_XML);
    L_URL          := 'http://10.32.36.15:7201/assinaturaws/services/Assinatura';
    --(SUBSTITUIR PARTE DESTACADA PELO ENDEREÇO QUE SERÁ UTILIZADO)
    L_NAMESPACE   := 'xmlns="http://DefaultNamespace"';
    L_METHOD      := 'getAssinatura';
    L_SOAP_ACTION := 'http://10.32.36.15:7201/assinaturaws/services/Assinatura?wsdl';
    --(SUBSTITUIR PARTE DESTACADA PELO ENDEREÇO QUE SERÁ UTILIZADO)
    L_RESULT_NAME := 'getAssinaturaReturn';
  
    L_REQUEST := SOAP_API.NEW_REQUEST(P_METHOD    => L_METHOD,
                                      P_NAMESPACE => L_NAMESPACE);
  
    SOAP_API.ADD_PARAMETER(P_REQUEST => L_REQUEST,
                           P_NAME    => 'xml',
                           P_TYPE    => 'xsd:string',
                           P_VALUE   => L_PARAM_ENCODE);
  
    L_RESPONSE := SOAP_API.INVOKE(P_REQUEST => L_REQUEST,
                                  P_URL     => L_URL,
                                  P_ACTION  => L_SOAP_ACTION);
  
    L_RETURN := SOAP_API.GET_RETURN_VALUE(P_RESPONSE  => L_RESPONSE,
                                          P_NAME      => L_RESULT_NAME,
                                          P_NAMESPACE => L_NAMESPACE);
  
    --DBMS_OUTPUT.PUT_LINE(DBMS_XMLGEN.CONVERT(L_RETURN, 1));
  
    RETURN DBMS_XMLGEN.CONVERT(L_RETURN, 1);
  END FC_ASSINA_XML;
  
  
  --Obtém dados do processo 
  PROCEDURE SP_RET_INFO_PROCESSO(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE,
                                P_CUR_CTR_PROCESSO OUT ESOCIAL.TSOC_CTR_PROCESSO%ROWTYPE, 
                                P_ID_EVENTO OUT ESOCIAL.TSOC_PAR_EVENTO.ID_EVENTO%TYPE,
                                P_ID_ORIGEM OUT ESOCIAL.TSOC_PAR_ORIGEM.ID_ORIGEM%TYPE,
                                P_TABELA OUT ESOCIAL.TSOC_PAR_EVENTO.NOM_TABELA%TYPE,
                                P_COD_TIPO_EVENTO OUT ESOCIAL.TSOC_PAR_EVENTO.COD_EVENTO%TYPE)
  IS 
  BEGIN 
  
    --OBTÉM INFORMAÇÕES DO AGENDAMENTO   
    SELECT *
      INTO P_CUR_CTR_PROCESSO
      FROM ESOCIAL.TSOC_CTR_PROCESSO CP
     WHERE CP.ID_CTR_PROCESSO = P_ID_CTR_PROCESSO;
  
    --OBTÉM ORIGEM E EVENTO RELACIONADO   
    SELECT ID_EVENTO, ID_ORIGEM
      INTO P_ID_EVENTO, P_ID_ORIGEM
      FROM ESOCIAL.TSOC_PAR_PROCESSO PP
     WHERE PP.ID_PROCESSO = P_CUR_CTR_PROCESSO.ID_PROCESSO;
  
    --OBTÉM A TABELA DE EVENTOS. 
    SELECT E.NOM_TABELA, E.COD_EVENTO
      INTO P_TABELA, P_COD_TIPO_EVENTO
      FROM ESOCIAL.TSOC_PAR_EVENTO E
     WHERE E.COD_INS = P_CUR_CTR_PROCESSO.COD_INS
       AND E.ID_EVENTO = P_ID_EVENTO;
       
END SP_RET_INFO_PROCESSO;

  

  --FAZ A LEITURA DO AGENDAMENTO DE PROCESSOS, E REALIZA A ASSINATURA DOS EVENTOS INDICADOS

  PROCEDURE SP_DY_CURSOR(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE,
                         P_ASSINATURA      IN OUT NOCOPY T_ASSINATURA,
                         P_TABELA          OUT NOCOPY VARCHAR2,
                         P_COD_TIPO_EVENTO OUT NOCOPY NUMBER) IS
  
    V_SQL_STMT     VARCHAR2(4000);
    V_SQL_STMT2    VARCHAR2(4000);
    V_CTR_PROCESSO ESOCIAL.TSOC_CTR_PROCESSO%ROWTYPE;
    V_ID_EVENTO    ESOCIAL.TSOC_PAR_EVENTO.ID_EVENTO%TYPE;
    V_ID_ORIGEM    ESOCIAL.TSOC_PAR_ORIGEM.ID_ORIGEM%TYPE;
    V_MIN_ID_PK    NUMBER(16);
    V_MAX_ID_PK    NUMBER(16);
  BEGIN
  
       
     SP_RET_INFO_PROCESSO(P_ID_CTR_PROCESSO,
                          V_CTR_PROCESSO, 
                          V_ID_EVENTO,
                          V_ID_ORIGEM,
                          P_TABELA,
                          P_COD_TIPO_EVENTO);
  
    --Caso não possua paralelismo, considerar do menor ao maior valor
    --de ID_PK disponível para processamento       
    IF V_CTR_PROCESSO.FAIXA_INI IS NULL OR V_CTR_PROCESSO.FAIXA_FIM IS NULL THEN
      V_SQL_STMT2 := 'SELECT MIN(ID_PK), MAX(ID_PK) FROM ' || P_TABELA ||
                     ' WHERE CTR_FLG_STATUS = ''AA''';
      EXECUTE IMMEDIATE V_SQL_STMT2
        INTO V_MIN_ID_PK, V_MAX_ID_PK;
    ELSE
      V_MIN_ID_PK := V_CTR_PROCESSO.FAIXA_INI;
      V_MAX_ID_PK := V_CTR_PROCESSO.FAIXA_FIM;
    END IF;
  
    --MONTA O CURSOR PARA ASSINATURA   
    V_SQL_STMT := 'SELECT ID_PK,XML_ENVIO,COD_INS FROM ' || P_TABELA ||
                  ' WHERE CTR_FLG_STATUS = ''AA''
                AND ID_PK BETWEEN ' || V_MIN_ID_PK ||
                  ' AND ' || V_MAX_ID_PK;
  
    OPEN P_ASSINATURA FOR V_SQL_STMT;
  
  END SP_DY_CURSOR;

  PROCEDURE SP_GERA_ASSINATURA(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE) IS
  
    V_ASSINATURA         T_ASSINATURA;
    V_XML_ENVIO_ASSINADO CLOB;
    V_TABELA             VARCHAR2(100);
    V_ID_PK              NUMBER(16);
    V_XML_ENVIO          CLOB;
    V_COD_INS            NUMBER;
    V_COD_TIP_EVENTO     ESOCIAL.TSOC_PAR_EVENTO.COD_EVENTO%TYPE;
  BEGIN
  
     
    
   
    SP_SETA_PROCESSO(P_ID_CTR_PROCESSO, 'INICIO_PROCESSAMENTO');
    --Monta o cursor
    SP_DY_CURSOR(P_ID_CTR_PROCESSO,
                 V_ASSINATURA,
                 V_TABELA,
                 V_COD_TIP_EVENTO);
  
    LOOP
      FETCH V_ASSINATURA
        INTO V_ID_PK, V_XML_ENVIO, V_COD_INS;
    
      --Chama o método que faz a assinatura. 
      --Habilitar quando o método estiver disponível no servidor. 
      BEGIN
        V_XML_ENVIO_ASSINADO := FC_ASSINA_XML(V_XML_ENVIO);

     --Marca status AL (Aguardando Lote) no evento. 
      SP_ATUALIZA_EVENTO(V_ID_PK,
                         V_TABELA,
                         V_XML_ENVIO_ASSINADO,
                         V_COD_INS,
                         'AL');

      EXCEPTION
        WHEN OTHERS THEN
          GB_REC_ERRO.COD_INS           := V_COD_INS;
          GB_REC_ERRO.ID_CAD            := V_ID_PK;
          GB_REC_ERRO.NOM_PROCESSO      := 'FC_ASSINA_XML';
          GB_REC_ERRO.TIPO_EVENTO       := V_COD_TIP_EVENTO;
          GB_REC_ERRO.DESC_ERRO         := 'ERRO AO RETORNAR ASSINATURA';
          GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
          GB_REC_ERRO.DES_IDENTIFICADOR := V_TABELA;
          GB_REC_ERRO.FLG_TIPO_ERRO     := 'E';
          SP_GERA_LOG_ERRO(P_ID_CTR_PROCESSO);
          --Marca status EA (Erro de Assinatura) no evento. 
          SP_ATUALIZA_EVENTO(V_ID_PK,
                             V_TABELA,
                             V_XML_ENVIO_ASSINADO,
                             V_COD_INS,
                             'EA');
      END;
    
     
    
      SP_SETA_PROCESSO(P_ID_CTR_PROCESSO, 'ATUALIZA_QUANTIDADE');
    
      EXIT WHEN V_ASSINATURA%NOTFOUND;
    END LOOP;
  
    SP_SETA_PROCESSO(P_ID_CTR_PROCESSO, 'FIM_PROCESSAMENTO');
  
  EXCEPTION
    WHEN OTHERS THEN
      GB_REC_ERRO.COD_INS           := V_COD_INS;
      GB_REC_ERRO.ID_CAD            := V_ID_PK;
      GB_REC_ERRO.NOM_PROCESSO      := 'SP_DY_CURSOR';
      GB_REC_ERRO.TIPO_EVENTO       := V_COD_TIP_EVENTO;
      GB_REC_ERRO.DESC_ERRO         := 'ERRO NO PROCESSO DE ASSINATURA';
      GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
      GB_REC_ERRO.DES_IDENTIFICADOR := V_TABELA;
      GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
      SP_GERA_LOG_ERRO(P_ID_CTR_PROCESSO);
      SP_SETA_PROCESSO(P_ID_CTR_PROCESSO, 'ERRO');
    
  END SP_GERA_ASSINATURA;

  PROCEDURE SP_ATUALIZA_EVENTO(P_ID_PK        IN NUMBER,
                               P_TABELA       IN VARCHAR2,
                               P_XML_ASSINADO IN CLOB,
                               P_COD_INS      IN NUMBER,
                               P_TIP_ATU      IN VARCHAR2 --EA, AL
                               ) IS
  
  BEGIN
  
    IF P_TIP_ATU = 'AL' THEN

      IF P_TABELA = 'TSOC_2410_BENEFICIO_INI' THEN
        UPDATE ESOCIAL.TSOC_2410_BENEFICIO_INI
           SET XML_ENVIO       = P_XML_ASSINADO,
               CTR_FLG_STATUS  = 'AL',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;
      
      ELSIF P_TABELA = 'TSOC_2400_BENEFICIARIO_INI' THEN
        UPDATE ESOCIAL.TSOC_2400_BENEFICIARIO_INI
           SET XML_ENVIO       = P_XML_ASSINADO,
               CTR_FLG_STATUS  = 'AL',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;
           
        ELSIF P_TABELA = 'TSOC_2405_BENEFICIARIO_ALT' THEN
        UPDATE ESOCIAL.TSOC_2405_BENEFICIARIO_ALT
           SET XML_ENVIO       = P_XML_ASSINADO,
               CTR_FLG_STATUS  = 'AL',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;     
           
           ELSIF P_TABELA = 'TSOC_2416_BENEFICIO_ALT' THEN
        UPDATE ESOCIAL.TSOC_2416_BENEFICIO_ALT
           SET XML_ENVIO       = P_XML_ASSINADO,
               CTR_FLG_STATUS  = 'AL',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;     
           
         ELSIF P_TABELA = 'TSOC_2420_BENEFICIO_TERMINO' THEN
        UPDATE ESOCIAL.TSOC_2420_BENEFICIO_TERMINO
           SET XML_ENVIO       = P_XML_ASSINADO,
               CTR_FLG_STATUS  = 'AL',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;     
           
           ELSIF P_TABELA = 'TSOC_1000_EMPREGADOR' THEN
        UPDATE ESOCIAL.TSOC_1000_EMPREGADOR
           SET XML_ENVIO       = P_XML_ASSINADO,
               CTR_FLG_STATUS  = 'AL',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;   

         ELSIF P_TABELA = 'TSOC_1010_RUBRICA' THEN
        UPDATE ESOCIAL.TSOC_1010_RUBRICA
           SET XML_ENVIO       = P_XML_ASSINADO,
               CTR_FLG_STATUS  = 'AL',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;   

      END IF;

   
    
    ELSE
    
      IF
      
       P_TABELA = 'TSOC_2410_BENEFICIO_INI' THEN
        UPDATE ESOCIAL.TSOC_2410_BENEFICIO_INI
           SET CTR_FLG_STATUS  = 'EA',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;
      
      ELSIF P_TABELA = 'TSOC_2400_BENEFICIARIO_INI' THEN
        UPDATE TSOC_2400_BENEFICIARIO_INI
           SET CTR_FLG_STATUS  = 'EA',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;
           
        
      ELSIF P_TABELA = 'TSOC_2405_BENEFICIARIO_ALT' THEN
        UPDATE TSOC_2405_BENEFICIARIO_ALT
           SET CTR_FLG_STATUS  = 'EA',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;
           
       ELSIF P_TABELA = 'TSOC_2416_BENEFICIO_ALT' THEN
        UPDATE TSOC_2416_BENEFICIO_ALT
           SET CTR_FLG_STATUS  = 'EA',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;    
      
       ELSIF P_TABELA = 'TSOC_2420_BENEFICIO_TERMINO' THEN
        UPDATE TSOC_2420_BENEFICIO_TERMINO
           SET CTR_FLG_STATUS  = 'EA',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;    
      
      ELSIF P_TABELA = 'TSOC_1000_EMPREGADOR' THEN
        UPDATE TSOC_1000_EMPREGADOR
           SET CTR_FLG_STATUS  = 'EA',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;  

      ELSIF P_TABELA = 'TSOC_1010_RUBRICA' THEN
        UPDATE TSOC_1010_RUBRICA
           SET CTR_FLG_STATUS  = 'EA',
               DAT_ULT_ATU     = SYSDATE,
               NOM_USU_ULT_ATU = 'ESOCIAL',
               NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
         WHERE ID_PK = P_ID_PK
           AND COD_INS = P_COD_INS;  
           

           
      END IF;
    
   END IF;
  
    COMMIT;
  
    /* EXECUTE IMMEDIATE V_UPDATE_STMT; */
  
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
  
  
  
--Função que remove assinatura  
--Se informado o ID_PK, remove a assinatura de um único evento
--Caso seja nulo, remove todas as assinaturas 
--Remove apenas a assinaturas de eventos que a contenham, e com status Aguardando Lote
PROCEDURE SP_REMOVE_ASSINATURA(P_ID_PK IN NUMBER, 
                               P_TABELA IN VARCHAR) 
                               
IS 
V_UPDATE_STMT CLOB;  
   BEGIN         
   
  V_UPDATE_STMT :=
      'UPDATE '|| P_TABELA ||'
        SET XML_ENVIO = SUBSTR(XML_ENVIO, 1, INSTR(XML_ENVIO, '|| CHR(39) || '<Signature' || CHR(39)
        ||' ) - 1) ||'|| CHR(39) || '</eSocial>'|| CHR(39) ||
      ', DAT_ULT_ATU = ' || CHR(39) ||  SYSDATE || CHR(39) ||
      ', NOM_USU_ULT_ATU = ' || CHR(39) ||'ESOCIAL' || CHR(39) ||
      ', NOM_PRO_ULT_ATU = ' || CHR(39) ||'SP_REMOVE_ASSINATURA'|| CHR(39) ||
      ', CTR_FLG_STATUS = ' || CHR(39) || 'AA' || CHR(39) ||
      ' WHERE (ID_PK = ' || P_ID_PK || ' OR ' || P_ID_PK ||' IS NULL) ' ||
      ' AND XML_ENVIO LIKE '|| CHR(39) ||'%<Signature%'|| CHR(39) || 
      ' AND CTR_FLG_STATUS IN (' || CHR(39) || 'AL' || CHR(39) || ',' || CHR(39) || 'EL' || CHR(39) || ')';
   
     EXECUTE IMMEDIATE V_UPDATE_STMT;     
    
END SP_REMOVE_ASSINATURA; 



END PAC_ESOCIAL_ASSINATURA;
/
