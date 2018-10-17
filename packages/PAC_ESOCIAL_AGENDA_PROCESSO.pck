CREATE OR REPLACE PACKAGE PAC_ESOCIAL_AGENDA_PROCESSO IS

  /*
  *************** INICIO_HELP: ****************
  ----------------------------------------------------

  Nome:
  PAC_ESOCIAL_AGENDA_PROCESSO

  Propósito:
  Package para agendar processos por faixa.

  Utilização:
  Utilizada por todos os objetos e transações que necessitem destas rotinas de uso geral.

  ----------------------------------------------------
  ***************** FINAL_HELP: *****************/
  
  ERRO           EXCEPTION;
  V_ERRO         NUMBER;
  V_DES_ERRO     VARCHAR2(2000);
  V_MAX_PARALELO NUMBER;
  
  -- Definicão de Variaveis para faixa 
  TYPE tp_list_faixa IS RECORD (
     qtd_paralelo number,
     qtd_reg      number,
     faixa_ini    number,
     faixa_fim    number
   );
   
  TYPE table_list_faixa IS TABLE OF tp_list_faixa INDEX BY BINARY_INTEGER;   
  v_list_faixa   table_list_faixa;  


  /**********************************************************************
    Nome:
    Procedure SP_AGENDA

    Propósito:
    Realizar inclusão de um registro na tabela de agendamento de processos "tsoc_ctr_processo",
    para controle e paralelismo do processamento.

    Parâmetros:
    P_COD_ERRO: Retorna o Código de Erro
    P_MSG_ERRO: Retorna a Mensagem de Erro
  */

  PROCEDURE SP_AGENDA(P_COD_ERRO   OUT NUMBER,
                      P_MSG_ERRO   OUT VARCHAR2);

  /**********************************************************************
    Nome:
    Função FC_EXISTE_PROCESSO

    Propósito:
    Verificar se existe processo parametrizado para agendar.

    Parâmetros:
  */

  FUNCTION FC_EXISTE_PROCESSO RETURN BOOLEAN;                      
  

  /**********************************************************************
    Nome:
    Função FC_EXISTE_EVENTO

    Propósito:
    Verificar se existe evento para agendar.

    Parâmetros:
  */

  FUNCTION FC_EXISTE_EVENTO RETURN BOOLEAN;
  
  /**********************************************************************
    Nome:
    Função FC_EXISTE_AGENTE

    Propósito:
    Verificar se existe processo sem paralelismo para agendar.

    Parâmetros:
  */

  FUNCTION FC_EXISTE_AGENTE RETURN BOOLEAN;  
  
  /**********************************************************************
    Nome:
    Função FC_POSSO_AGENDA

    Propósito:
    Verificar se posso agendar o processo.

    Parâmetros:
  */

  FUNCTION FC_POSSO_AGENDA(P_FLG_PARALELO IN VARCHAR2)  RETURN BOOLEAN;

  /**********************************************************************
    Nome:
    Procedure SP_GRAVA_EVENTO

    Propósito:
    Incluir eventos por faixa.

    Parâmetros:
  */

  PROCEDURE SP_GRAVA_EVENTO;  
   
  /**********************************************************************
    Nome:
    Procedure SP_GRAVA_AGENTE

    Propósito:
    Incluir na agenda os processos sem parelelismo.

    Parâmetros:
  */

  PROCEDURE SP_GRAVA_AGENTE;   
  
  /**********************************************************************
    Nome:
    Procedure SP_INCLUI_CTR_PROCESSO

    Propósito:
    Incluir na tabale tsoc_ctr_processo.

    Parâmetros:
    P_ID_PROCESSO: Número do processo
    P_ID_PERIODO: Identificação do período
    P_COD_INS: código do Instituto
    P_FAIXA_INI: Inicio da faixa
    P_FAIXA_FIM: Fim da faixa
  */
  PROCEDURE SP_INCLUI_CTR_PROCESSO(P_ID_PROCESSO     NUMBER,
                                   P_ID_PERIODO      NUMBER,
                                   P_COD_INS         NUMBER,
                                   P_FAIXA_INI       NUMBER DEFAULT NULL,
                                   P_FAIXA_FIM       NUMBER DEFAULT NULL
                                  );  
                                  

  /**********************************************************************
    Nome:
    Procedure SP_ATUALIZA_STATUS_PROCESSO

    Propósito:
    Atualiza o status da parametrização do processo.

    Parâmetros:
    P_COD_INS: Código do Instituto
    P_ID_PROCESSO: Número identificador da processo
  */
  PROCEDURE SP_ATUALIZA_STATUS_PROCESSO(P_COD_INS     IN NUMBER,
                                        P_ID_PROCESSO IN NUMBER,
                                        P_FLG_STATUS  IN VARCHAR2);                                  
   
  
  /**********************************************************************
    Nome:
    Procedure SP_DIVIDE_EVENTO

    Propósito:
    Divide o evente em faixas de processamento

    Parâmetros:
    P_COD_INS: Código do Instituto
    P_ID_PROCESSO: Número identificador da processo
  */
  PROCEDURE SP_DIVIDE_EVENTO(P_COD_INS     IN NUMBER,
                             P_ID_PROCESSO IN NUMBER,
                             P_ID_PERIODO  IN NUMBER);  
  /**********************************************************************
    Nome:
    Procedure SP_GRAVA_ERRO_PROCESSO

    Propósito:
    Realizar inclusão de um registro na tabela de analise de processo tabela,
    para armazenamento de erros do processamento.

    Parâmetros:
    P_COD_INS: Código do Instituto
    P_ID_PROCESSO: Número identificador da processo
    P_ID_ERRO: Retorna o Código de Erro
    P_DES_ERRO: Conteúdo do arquivo de process
    P_PARAM01: Parametro de apoio 1
    P_PARAM02: Parametro de apoio 2
    P_PARAM03: Parametro de apoio 3    
  */

  PROCEDURE SP_GRAVA_ERRO_PROCESSO(P_COD_INS     IN NUMBER,
                                   P_ID_PROCESSO IN NUMBER,
                                   P_ID_ERRO     IN NUMBER,
                                   P_DES_ERRO    IN VARCHAR2 DEFAULT NULL,
                                   P_PARAM01     IN VARCHAR2 DEFAULT NULL,
                                   P_PARAM02     IN VARCHAR2 DEFAULT NULL,
                                   P_PARAM03     IN VARCHAR2 DEFAULT NULL);

END PAC_ESOCIAL_AGENDA_PROCESSO;
/
CREATE OR REPLACE PACKAGE BODY PAC_ESOCIAL_AGENDA_PROCESSO IS

  /**********************************************************************
  PROCEDURE
  SP_GRAVA_ERRO_PROCESSO.

  **********************************************************************/
  PROCEDURE SP_AGENDA(P_COD_ERRO   OUT NUMBER,
                      P_MSG_ERRO   OUT VARCHAR2) IS

  BEGIN 
    --Verificar se tem processos para agendar.
    IF FC_EXISTE_PROCESSO THEN
      DBMS_OUTPUT.put_line('TEM PROCESSOS PARA AGENDAR!');
 
      --Verificar se tem processo sem paralelismo para agendar.
      IF FC_EXISTE_AGENTE THEN
        DBMS_OUTPUT.put_line('TEM PROCESSO SEM PARALELISMO PARA AGENDAR!');
        --Verificar se posso agendar o processo.
        IF FC_POSSO_AGENDA('N') THEN
          DBMS_OUTPUT.put_line('SIM PODE AGENDAR!');
          SP_GRAVA_AGENTE;
        ELSE
          DBMS_OUTPUT.put_line('NÃO PODE AGENDAR!');
        END IF;--agendar  
      END IF;
      
      --Verificar se tem eventos para agendar.
      IF FC_EXISTE_EVENTO THEN
        DBMS_OUTPUT.put_line('TEM EVENTOS PARA AGENDAR!');
        --Verificar se posso agendar o processo.
        IF FC_POSSO_AGENDA('S') THEN
          DBMS_OUTPUT.put_line('SIM PODE AGENDAR!');
          SP_GRAVA_EVENTO;
        ELSE
          DBMS_OUTPUT.put_line('NÃO PODE AGENDAR!');
        END IF;--agendar  
      ELSE
        DBMS_OUTPUT.put_line('NÃO TEM EVENTOS PARA AGENDAR!');
      END IF;--eventos  
    ELSE
      DBMS_OUTPUT.put_line('NÃO TEM PROCESSOS PARA AGENDAR!');
    END IF;--processos  

  EXCEPTION
    WHEN ERRO THEN
      P_COD_ERRO := V_ERRO;
      P_MSG_ERRO := V_DES_ERRO;
    WHEN OTHERS THEN
      P_COD_ERRO := SQLCODE;
      P_MSG_ERRO := SQLERRM;
  END SP_AGENDA;     

  /**********************************************************************
  FUNCTION
  FC_EXISTE_PROCESSO.

  **********************************************************************/
  FUNCTION FC_EXISTE_PROCESSO RETURN BOOLEAN IS    
    v_qtd number := 0;
  BEGIN
    select count(*)
      into v_qtd
      from esocial.tsoc_par_processo p
         , esocial.tsoc_par_evento e
     where p.cod_ins = e.cod_ins
       and p.id_evento = e.id_evento
       and p.flg_status = 'A'
       and not exists (select 1
                         from esocial.tsoc_ctr_processo c
                        where c.cod_ins = p.cod_ins
                          and c.id_processo = p.id_processo
                          and c.flg_status in ('P')); 
       
    if v_qtd > 0 then
      return true;
    else
      return false;
    end if; 
     
  EXCEPTION
    WHEN OTHERS THEN
      V_ERRO     := SQLCODE;
      V_DES_ERRO := SQLERRM || 'ERRO NA EXECUÇÃO DA FUNÇÃO: FC_EXISTE_PROCESSO';   
      RAISE ERRO;    
  END FC_EXISTE_PROCESSO;                      

  /**********************************************************************
  FUNCTION
  FC_EXISTE_EVENTO.

  **********************************************************************/
  FUNCTION FC_EXISTE_EVENTO RETURN BOOLEAN IS
       
  CURSOR C_PROCESSO IS
      select p.cod_ins, p.id_processo, e.nom_tabela, p.id_programa, p.qtd_paralelo
      from esocial.tsoc_par_processo p
         , esocial.tsoc_par_evento e
     where p.cod_ins = e.cod_ins
       and p.id_evento = e.id_evento
       and p.flg_status = 'A'
       and p.flg_paralelo = 'S'
     order by id_processo;

  CURSOR C_PROGRAMA(V_ID_PROG NUMBER) IS
      select c.coluna, c.operador, c.valor, c.tipo_valor
      from esocial.tsoc_par_programa g
         , esocial.tsoc_par_cond_programa c
     where g.cod_ins = c.cod_ins
       and g.id_programa = c.id_programa
       and g.id_programa = V_ID_PROG
     order by c.num_ordem; 
  
  V_COMANDO       VARCHAR2(32000);
  V_CONDICAO      VARCHAR2(4000);
  V_QTD_REG       NUMBER;
  V_TOT_REG       NUMBER; 
  V_INI_REG       NUMBER; 
  V_FIM_REG       NUMBER; 
  V_TEM_CONDICAO  BOOLEAN;
  V_FAIXA_FIM     NUMBER;

  BEGIN

    V_TOT_REG := 0;
    V_FAIXA_FIM :=0;

    --busca tabela do processo
    FOR X IN C_PROCESSO LOOP
      
    select NVL(MAX(FAIXA_FIM),0)
      INTO V_FAIXA_FIM
      from esocial.tsoc_ctr_processo c
     where c.cod_ins = X.COD_INS
       and c.id_processo = X.ID_PROCESSO
       and c.flg_status in ('A', 'P');
      
      --limpar erro
      DELETE FROM esocial.TSOC_CTR_ERRO
      WHERE COD_INS = X.COD_INS 
      AND ID_PROCESSO = X.ID_PROCESSO;
      
      V_INI_REG := 0;
      V_FIM_REG := 0;
      V_QTD_REG := 0;
      V_COMANDO := null;
      
      V_COMANDO := 
        ' SELECT COUNT(*), MIN(ID_PK), MAX(ID_PK)'||
        '   FROM '||X.NOM_TABELA;
        
      BEGIN
        execute immediate V_COMANDO;
      EXCEPTION
        WHEN OTHERS THEN
          SP_GRAVA_ERRO_PROCESSO(X.COD_INS,
                                 X.ID_PROCESSO,
                                 2,-- NÃO EXISTE TABELA
                                 NULL,
                                 X.NOM_TABELA);
          DBMS_OUTPUT.PUT_LINE('TABELA: '||X.NOM_TABELA||' NÃO EXISTE');
          GOTO FIM;
      END;    
      
      V_CONDICAO     := NULL;
      V_TEM_CONDICAO := FALSE;
      --busca condição do processo
      FOR Y IN C_PROGRAMA(X.ID_PROGRAMA) LOOP
        
        V_TEM_CONDICAO := TRUE;
        
        IF V_CONDICAO IS NULL THEN
          V_CONDICAO :=  ' WHERE ';
        END IF;
       
        V_COMANDO := V_COMANDO||
        V_CONDICAO||Y.COLUNA||' '||Y.OPERADOR;
        
        IF Y.TIPO_VALOR = 'C' THEN 
          V_COMANDO := V_COMANDO||' '''||Y.VALOR||'''';
        ELSIF Y.TIPO_VALOR = 'N' THEN   
          V_COMANDO := V_COMANDO||' '||TO_NUMBER(Y.VALOR);
        ELSIF Y.TIPO_VALOR = 'D' THEN   
          V_COMANDO := V_COMANDO||' '||TO_DATE(Y.VALOR,'DD/MM/YYYY');
        END IF;
                  
        V_CONDICAO := ' AND ';
      END LOOP;  
      
      IF V_TEM_CONDICAO THEN
        
        V_COMANDO := V_COMANDO||' AND ID_PK > '||V_FAIXA_FIM;
        
        BEGIN
          execute immediate V_COMANDO INTO V_QTD_REG, V_INI_REG, V_FIM_REG;       
        EXCEPTION
          WHEN OTHERS THEN
            V_ERRO     := 1002;
            V_DES_ERRO := SQLERRM||' ERRO NA EXECUÇÃO DA FUNÇÃO: FC_EXISTE_EVENTO - V_COMANDO';
            SP_GRAVA_ERRO_PROCESSO(X.COD_INS,
                                   X.ID_PROCESSO,
                                   V_ERRO,-- ERRO ORA
                                   V_DES_ERRO);
                                   
          DBMS_OUTPUT.PUT_LINE('COMANDO: '||V_COMANDO);  
        END;
        
        --Se não existe eventos suspente parametrização
        IF V_QTD_REG = 0 THEN
          RETURN FALSE;
          --SP_ATUALIZA_STATUS_PROCESSO(X.COD_INS, X.ID_PROCESSO, 'S');
        ELSE
          --Incluir dados na tabale em memória.
          V_LIST_FAIXA(X.ID_PROCESSO).QTD_PARALELO := X.QTD_PARALELO;
          V_LIST_FAIXA(X.ID_PROCESSO).QTD_REG      := V_QTD_REG;
          V_LIST_FAIXA(X.ID_PROCESSO).FAIXA_INI    := V_INI_REG;
          V_LIST_FAIXA(X.ID_PROCESSO).FAIXA_FIM    := V_FIM_REG;
          
        END IF;
        
        --Atualiza total de registros.
        V_TOT_REG := V_TOT_REG+V_QTD_REG;
      END IF;
      <<FIM>> 
      NULL;        
    END LOOP;   
    
    IF V_TOT_REG > 0 THEN
      if sql%found then
        COMMIT;
      end if;  
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;  

  EXCEPTION
    WHEN ERRO THEN
      RAISE;
    WHEN OTHERS THEN
      V_ERRO     := SQLCODE;
      V_DES_ERRO := SQLERRM || 'ERRO NA EXECUÇÃO DA FUNÇÃO: FC_EXISTE_EVENTO';   
      RAISE ERRO;        
  END FC_EXISTE_EVENTO;

  /**********************************************************************
  FUNCTION
  FC_EXISTE_AGENTE.

  **********************************************************************/
  FUNCTION FC_EXISTE_AGENTE RETURN BOOLEAN IS
    CURSOR C_PROCESSO IS
      select count(*) qtd, p.cod_ins, p.id_processo
        from tsoc_par_processo p
           , tsoc_par_evento e
       where p.cod_ins = e.cod_ins
         and p.id_evento = e.id_evento
         and p.flg_status = 'A'
         and p.flg_paralelo = 'N'
         and not exists (select 1
                           from tsoc_ctr_processo c
                          where c.cod_ins = p.cod_ins
                            and c.id_processo = p.id_processo
                            and c.flg_status in ('A','P'))
      group by p.cod_ins, p.id_processo; 
     v_qtd number := 0;
  BEGIN
     
    for reg in c_processo loop
      --limpar erro
      delete from tsoc_ctr_erro
      where cod_ins = reg.cod_ins
      and id_processo = reg.id_processo;
      
      v_qtd := v_qtd + reg.qtd;
    end loop;   
      
    if v_qtd > 0 then
      if sql%found then
        COMMIT;
      end if;
      return true;
    else
      return false;
    end if;   
     
  EXCEPTION
    WHEN OTHERS THEN
      V_ERRO     := SQLCODE;
      V_DES_ERRO := SQLERRM || 'ERRO NA EXECUÇÃO DA FUNÇÃO: FC_EXISTE_AGENTE';   
      RAISE ERRO;    
  END FC_EXISTE_AGENTE;

  /**********************************************************************
  FUNCTION
  FC_EXISTE_EVENTO.

  **********************************************************************/
  FUNCTION FC_POSSO_AGENDA(P_FLG_PARALELO IN VARCHAR2) RETURN BOOLEAN IS  
    CURSOR C_PROCESSO IS
      select distinct p.id_processo, p.qtd_paralelo, p.flg_paralelo
        from tsoc_par_processo p, tsoc_par_evento e
       where p.cod_ins = e.cod_ins
         and p.id_evento = e.id_evento
         and p.flg_status = 'A'
         and p.flg_paralelo = P_FLG_PARALELO
         and (not exists (select 1
                           from tsoc_ctr_processo c
                          where c.cod_ins = p.cod_ins
                            and c.id_processo = p.id_processo
                            and c.flg_status in ('A', 'P'))
              OR P_FLG_PARALELO = 'S' AND not exists (select 1
                           from tsoc_ctr_processo c
                          where c.cod_ins = p.cod_ins
                            and c.id_processo = p.id_processo
                            and c.flg_status in ( 'P')))
       order by 1;
       
  v_qtd_processo NUMBER;
  
  BEGIN
    
    for reg in c_processo loop
      
      if reg.flg_paralelo = 'N' then
        RETURN TRUE;        
      end if;  

      v_qtd_processo := 0;
      select count(*)
        into v_qtd_processo  
        from tsoc_ctr_processo c
       where c.id_processo = reg.id_processo;
       
      if reg.qtd_paralelo > v_qtd_processo then
        V_MAX_PARALELO := reg.qtd_paralelo - v_qtd_processo;
        RETURN TRUE;        
      end if;  
    end loop;
  
    RETURN FALSE; 
           
  EXCEPTION
    WHEN OTHERS THEN
      V_ERRO     := SQLCODE;
      V_DES_ERRO := SQLERRM || 'ERRO NA EXECUÇÃO DA FUNÇÃO: FC_POSSO_AGENDA';   
      RAISE ERRO;      
  END FC_POSSO_AGENDA;

  /**********************************************************************
  PROCEDURE
  SP_GRAVA_EVENTO.

  **********************************************************************/  
  PROCEDURE SP_GRAVA_EVENTO IS
    CURSOR C_PROCESSO IS
      select p.cod_ins, p.id_processo, p.id_origem, p.id_evento
        from tsoc_par_processo p, tsoc_par_evento e
       where p.cod_ins = e.cod_ins
         and p.id_evento = e.id_evento
         and p.flg_status = 'A'
         and p.flg_paralelo = 'S'
         /*and not exists (select 1
                           from tsoc_ctr_processo c
                          where c.cod_ins = p.cod_ins
                            and c.id_processo = p.id_processo
                            and c.flg_status in ('A', 'P')
                            and c.faixa_ini >= V_LIST_FAIXA(C.ID_PROCESSO).FAIXA_INI
                            and c.faixa_fim <= V_LIST_FAIXA(C.ID_PROCESSO).FAIXA_FIM*/
                            --)
       order by 2;

  V_ID_PERIODO TSOC_CTR_PERIODO.ID_PERIODO%TYPE;     
  V_EXISTE NUMBER := 0;  
  V_INI NUMBER := 0;  
  V_FIM NUMBER := 0;  
  
  BEGIN
    FOR REG IN C_PROCESSO LOOP
    
    
    --TESTE  
    V_INI:= v_list_faixa(reg.id_processo).faixa_ini;
    V_FIM:= v_list_faixa(reg.id_processo).faixa_fim;
      
      select COUNT(*)
        INTO V_EXISTE
        from tsoc_ctr_processo c
       where c.cod_ins = reg.cod_ins
         and c.id_processo = reg.id_processo
         and c.flg_status in ('A', 'P')
         and c.faixa_ini >= v_list_faixa(reg.id_processo).faixa_ini
         and c.faixa_fim <= v_list_faixa(reg.id_processo).faixa_fim;
      
      IF NOT V_EXISTE > 0 THEN
        --Buscar período aberto para agendamento
        BEGIN
          SELECT P.ID_PERIODO
            INTO V_ID_PERIODO
            FROM TSOC_CTR_PERIODO_DET P
           WHERE P.COD_INS = REG.COD_INS
             AND P.ID_EVENTO = REG.ID_EVENTO
             AND P.ID_ORIGEM = REG.ID_ORIGEM
             AND P.FLG_STATUS = 'A';
             
          --Dividir o evente em faixas de processamento
          SP_DIVIDE_EVENTO(REG.COD_INS, REG.ID_PROCESSO, V_ID_PERIODO);   
                  
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            SP_GRAVA_ERRO_PROCESSO(REG.COD_INS,
                                   REG.ID_PROCESSO,
                                   1,--NÃO EXISTE PERÍDO ABERTO
                                   NULL);
        END;
      END IF;                          
    END LOOP;

  EXCEPTION
    WHEN ERRO THEN
      RAISE;
    WHEN OTHERS THEN
      V_ERRO     := SQLCODE;
      V_DES_ERRO := SQLERRM || 'ERRO NA EXECUÇÃO DO PROCEDIMENTO: SP_GRAVA_AGENTE';   
      RAISE ERRO; 
  END SP_GRAVA_EVENTO;
   
  /**********************************************************************
  PROCEDURE
  SP_GRAVA_AGENTE.

  **********************************************************************/  
  PROCEDURE SP_GRAVA_AGENTE IS
    CURSOR C_PROCESSO IS
      select p.cod_ins, p.id_processo, p.id_origem, p.id_evento
        from tsoc_par_processo p, tsoc_par_evento e
       where p.cod_ins = e.cod_ins
         and p.id_evento = e.id_evento
         and p.flg_status = 'A'
         and p.flg_paralelo = 'N'
         and not exists (select 1
                           from tsoc_ctr_processo c
                          where c.cod_ins = p.cod_ins
                            and c.id_processo = p.id_processo
                            and c.flg_status in ('A', 'P'))
       order by 2;

  V_ID_PERIODO TSOC_CTR_PERIODO.ID_PERIODO%TYPE;       
  
  BEGIN
    FOR REG IN C_PROCESSO LOOP
      
      --Buscar período aberto para agendamento
      BEGIN
        SELECT P.ID_PERIODO
          INTO V_ID_PERIODO
          FROM TSOC_CTR_PERIODO_DET P
         WHERE P.COD_INS = REG.COD_INS
           AND P.ID_EVENTO = REG.ID_EVENTO
           AND P.ID_ORIGEM = REG.ID_ORIGEM
           AND P.FLG_STATUS = 'A';

        -- Incluí na tsoc_ctr_processo
        SP_INCLUI_CTR_PROCESSO(REG.ID_PROCESSO,
                               V_ID_PERIODO,
                               REG.COD_INS
                               );
        IF V_ERRO IS NOT NULL THEN
          RAISE ERRO;
        END IF;   
                
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          SP_GRAVA_ERRO_PROCESSO(REG.COD_INS,
                                 REG.ID_PROCESSO,
                                 1,--NÃO EXISTE PERÍDO ABERTO
                                 NULL);
      END;
                            
    END LOOP;

  EXCEPTION
    WHEN ERRO THEN
      RAISE;
    WHEN OTHERS THEN
      V_ERRO     := SQLCODE;
      V_DES_ERRO := SQLERRM || 'ERRO NA EXECUÇÃO DO PROCEDIMENTO: SP_GRAVA_AGENTE';   
      RAISE ERRO;       
  END SP_GRAVA_AGENTE;  
  
  /**********************************************************************
  PROCEDURE
  SP_INCLUI_CTR_PROCESSO.

  **********************************************************************/  
  PROCEDURE SP_INCLUI_CTR_PROCESSO(P_ID_PROCESSO     NUMBER,
                                   P_ID_PERIODO      NUMBER,
                                   P_COD_INS         NUMBER,
                                   P_FAIXA_INI       NUMBER DEFAULT NULL,
                                   P_FAIXA_FIM       NUMBER DEFAULT NULL
                                  ) IS
    
  BEGIN
    BEGIN
      INSERT INTO ESOCIAL.TSOC_CTR_PROCESSO
        (
          ID_CTR_PROCESSO,
          ID_PROCESSO,
          ID_PERIODO,
          COD_INS,
          FAIXA_INI,
          FAIXA_FIM,
          FLG_STATUS,
          DAT_ING,
          DAT_ULT_ATU,
          NOM_USU_ULT_ATU,
          NOM_PRO_ULT_ATU)
      VALUES
        (
        SEQ_SOC_CTR_PROCESSO.NEXTVAL, --ID_CTR_PROCESSO
        P_ID_PROCESSO,
        P_ID_PERIODO,
        P_COD_INS,
        P_FAIXA_INI,
        P_FAIXA_FIM,
        'A',--STATUS AGUANDANDO
        SYSDATE,
        SYSDATE,
        'PC_AGENDA',
        'SP_INCLUI_CTR_PROCESSO'
        );
        
    DBMS_OUTPUT.PUT_LINE(P_ID_PROCESSO||' - '||'IDENTIFICAÇÃO DO PROCESSO AGENDADO!');

    COMMIT;        
    
    EXCEPTION
      WHEN OTHERS THEN
        SP_GRAVA_ERRO_PROCESSO(P_COD_INS,
                               P_ID_PROCESSO,
                               0,--ERRO AO INCLUIR AGENDAMENTO
                               NULL);    
    END;  

    
  EXCEPTION
      WHEN OTHERS THEN
        V_ERRO         := 1002;
        V_DES_ERRO     := SQLERRM || ' ERRO DE INCLUSÃO NA CONTROLE PROCESSO.';
  END SP_INCLUI_CTR_PROCESSO;  
  
  /**********************************************************************
  PROCEDURE
  SP_ATUALIZA_STATUS_PROCESSO.

  **********************************************************************/
  PROCEDURE SP_ATUALIZA_STATUS_PROCESSO(P_COD_INS     IN NUMBER,
                                        P_ID_PROCESSO IN NUMBER,
                                        P_FLG_STATUS  IN VARCHAR2) IS

  BEGIN 
    
    UPDATE TSOC_PAR_PROCESSO P
       SET P.FLG_STATUS = P_FLG_STATUS
     WHERE P.COD_INS = P_COD_INS
       AND P.ID_PROCESSO = P_ID_PROCESSO;
       
    COMMIT; 
  
  EXCEPTION
    WHEN OTHERS THEN
      SP_GRAVA_ERRO_PROCESSO(P_COD_INS,
                             P_ID_PROCESSO,
                             3,--ERRO AO ATUALIZAR STATUS DO PROCESSO
                             NULL);    
  END SP_ATUALIZA_STATUS_PROCESSO;  

  /**********************************************************************
  PROCEDURE
  SP_ATUALIZA_STATUS_PROCESSO.

  **********************************************************************/  
  PROCEDURE SP_DIVIDE_EVENTO(P_COD_INS     IN NUMBER,
                             P_ID_PROCESSO IN NUMBER,
                             P_ID_PERIODO  IN NUMBER) IS
    V_QTD_PARALELO NUMBER;
    V_QTD_REG      NUMBER;
    V_FAIXA_INI    NUMBER; 
    V_FAIXA_FIM    NUMBER;
    V_FAIXA_FIM_ORIGEM NUMBER;
    V_QTD_POR_FAIXA NUMBER;
    V_COUNT         NUMBER := 0;

  BEGIN

    DBMS_OUTPUT.PUT_LINE(V_LIST_FAIXA(P_ID_PROCESSO).QTD_PARALELO);
    DBMS_OUTPUT.PUT_LINE(V_LIST_FAIXA(P_ID_PROCESSO).QTD_REG);
    DBMS_OUTPUT.PUT_LINE(V_LIST_FAIXA(P_ID_PROCESSO).FAIXA_INI);
    DBMS_OUTPUT.PUT_LINE(V_LIST_FAIXA(P_ID_PROCESSO).FAIXA_FIM);
    
    V_QTD_PARALELO := V_LIST_FAIXA(P_ID_PROCESSO).QTD_PARALELO;
    V_QTD_REG      := V_LIST_FAIXA(P_ID_PROCESSO).QTD_REG;
    V_FAIXA_INI    := V_LIST_FAIXA(P_ID_PROCESSO).FAIXA_INI; 
    V_FAIXA_FIM_ORIGEM    := V_LIST_FAIXA(P_ID_PROCESSO).FAIXA_FIM;
    
    
     V_QTD_POR_FAIXA := TRUNC((V_QTD_REG / V_QTD_PARALELO) - 1);

    FOR FX IN 1..V_MAX_PARALELO LOOP
      V_COUNT := V_COUNT + 1;
      V_FAIXA_FIM := (V_FAIXA_INI+V_QTD_POR_FAIXA);
      
      IF V_COUNT = V_QTD_PARALELO THEN
        --V_FAIXA_FIM := V_QTD_REG;
        V_FAIXA_FIM := V_FAIXA_FIM_ORIGEM;
      END IF; 
      
      DBMS_OUTPUT.PUT_LINE(V_COUNT); 
      
      DBMS_OUTPUT.PUT_LINE('INI: '||V_FAIXA_INI);
      DBMS_OUTPUT.PUT_LINE('FIM: '||V_FAIXA_FIM);    
    
          -- Incluí na tsoc_ctr_processo
        SP_INCLUI_CTR_PROCESSO(P_ID_PROCESSO,
                               P_ID_PERIODO,
                               P_COD_INS,
                               V_FAIXA_INI,
                               V_FAIXA_FIM
                               );
        IF V_ERRO IS NOT NULL THEN
          RAISE ERRO;
        END IF;   

      V_FAIXA_INI := V_FAIXA_INI+V_QTD_POR_FAIXA+1;           
    END LOOP;
          
  END SP_DIVIDE_EVENTO;                                   
                                        
  /**********************************************************************
  PROCEDURE
  SP_GRAVA_ERRO_PROCESSO.

  **********************************************************************/
  PROCEDURE SP_GRAVA_ERRO_PROCESSO(P_COD_INS     IN NUMBER,
                                   P_ID_PROCESSO IN NUMBER,
                                   P_ID_ERRO     IN NUMBER,
                                   P_DES_ERRO    IN VARCHAR2 DEFAULT NULL,
                                   P_PARAM01     IN VARCHAR2 DEFAULT NULL,
                                   P_PARAM02     IN VARCHAR2 DEFAULT NULL,
                                   P_PARAM03     IN VARCHAR2 DEFAULT NULL) IS

  BEGIN
    INSERT INTO ESOCIAL.TSOC_CTR_ERRO
        (COD_INS,
         ID_PROCESSO,
         ID_ERRO,
         DES_ERRO_ORA,
         DAT_ING,
         DAT_ULT_ATU,
         NOM_USU_ULT_ATU,
         NOM_PRO_ULT_ATU,
         PARAM01,
         PARAM02,
         PARAM03)
      VALUES
        (P_COD_INS,
         P_ID_PROCESSO,
         P_ID_ERRO,
         P_DES_ERRO,
         SYSDATE,
         SYSDATE,
         'PAC_ESOCIAL_AGENDA',
         'SP_GRAVA_ERRO_PROCESSO',
         P_PARAM01,
         P_PARAM02,
         P_PARAM03);
    
      COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      V_ERRO     := SQLCODE;
      V_DES_ERRO := SQLERRM || 'ERRO NA EXECUÇÃO DO PROCEDIMENTO: SP_GRAVA_ERRO_PROCESSO';   
      RAISE ERRO;       
  END SP_GRAVA_ERRO_PROCESSO;
 
END PAC_ESOCIAL_AGENDA_PROCESSO;
/
