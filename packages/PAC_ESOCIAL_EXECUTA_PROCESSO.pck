CREATE OR REPLACE PACKAGE PAC_ESOCIAL_EXECUTA_PROCESSO IS

  /*
  *************** INICIO_HELP: ****************
  ----------------------------------------------------

  Nome:
  PAC_ESOCIAL_EXECUTA_PROCESSO

  Propósito:
  Package para executar processos agendados por faixa.

  Utilização:
  Utilizada por todos os objetos e transações que necessitem destas rotinas de uso geral.

  ----------------------------------------------------
  ***************** FINAL_HELP: *****************/

  ERRO           EXCEPTION;
  V_ERRO         NUMBER;
  V_DES_ERRO     VARCHAR2(2000);
  --Quantidade de processo 
  V_QTD_PROCESSO NUMBER;
  V_IP_HOST      VARCHAR2(100);
  /*
    Nome:
    Procedure SP_EXECUTA

    Propósito:
    Realizar execução dos registros da tabela de controle.

    Parâmetros:
    P_COD_INS: Código do Instituto
    P_NUM: Número identificador da processo
  */

  PROCEDURE SP_EXECUTA(P_COD_ERRO   OUT NUMBER,
                       P_MSG_ERRO   OUT VARCHAR2);
                             
  /**********************************************************************
    Nome:
    Função FC_EXISTE_PROCESSO

    Propósito:
    Verificar se existe processos para executar.

    Parâmetros:
  */

  FUNCTION FC_EXISTE_PROCESSO(P_FLG_STATUS VARCHAR2) RETURN BOOLEAN;  

  /**********************************************************************
    Nome:
    Função FC_POSSO_EXECUTA

    Propósito:
    Verificar se existe processos para executar.

    Parâmetros:
  */
  FUNCTION FC_POSSO_EXECUTA(P_COD_INS         OUT NUMBER, 
                            P_ID_PROCESSO     OUT NUMBER, 
                            P_ID_CTR_PROCESSO OUT NUMBER) RETURN BOOLEAN;

  /**********************************************************************
    Nome:
    Procedure SP_EXEC_PROCESSO

    Propósito:
    Executar eventos agendads.

    Parâmetros:
  */
  PROCEDURE SP_EXEC_PROCESSO(P_COD_INS         NUMBER, 
                             P_ID_PROCESSO     NUMBER, 
                             P_ID_CTR_PROCESSO NUMBER);

  /**********************************************************************
    Nome:
    Procedure SP_GET_IP_HOST

    Propósito:
    Busca o ip da maquina.

    Parâmetros:
  */
  PROCEDURE SP_GET_IP_HOST; 
 
  /**********************************************************************
    Nome:
    Função FC_GET_DATA_EXECUCAO

    Propósito:
    Verificar se existe mes, semana, dia, hora, minuto para executar.

    Parâmetros:
  */
  FUNCTION FC_GET_DATA_EXECUCAO(P_ID_PROCESSO NUMBER) RETURN VARCHAR2;

END PAC_ESOCIAL_EXECUTA_PROCESSO;
/
CREATE OR REPLACE PACKAGE BODY PAC_ESOCIAL_EXECUTA_PROCESSO IS

  /**********************************************************************
  PROCEDURE
  SP_EXECUTA.

  **********************************************************************/
  PROCEDURE SP_EXECUTA(P_COD_ERRO   OUT NUMBER,
                             P_MSG_ERRO   OUT VARCHAR2) IS

  V_COD_INS          TSOC_CTR_PROCESSO.COD_INS%TYPE;
  V_ID_PROCESSO      TSOC_CTR_PROCESSO.ID_PROCESSO%TYPE;
  V_ID_CTR_PROCESSO  TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE;
  BEGIN
    --busca ip da maquina.
    SP_GET_IP_HOST;
    --Verificar se tem processos para executar.
    IF FC_EXISTE_PROCESSO('A') THEN
      DBMS_OUTPUT.PUT_LINE('TEM '||V_QTD_PROCESSO||' PROCESSOS PARA EXECUTAR!');
      --Verificar se tem processos em execução.
      IF FC_EXISTE_PROCESSO('P') THEN
        DBMS_OUTPUT.PUT_LINE('TEM '||V_QTD_PROCESSO||' PROCESSOS EM EXECUÇÃO!');
      ELSE
        DBMS_OUTPUT.PUT_LINE('TEM '||V_QTD_PROCESSO||' PROCESSOS EM EXECUÇÃO!');  
      END IF;
      --Verificar se posso executar os processos.
      IF FC_POSSO_EXECUTA(V_COD_INS, V_ID_PROCESSO, V_ID_CTR_PROCESSO) THEN
        DBMS_OUTPUT.PUT_LINE('SIM POSSO EXECUTAR');
        SP_EXEC_PROCESSO(V_COD_INS, V_ID_PROCESSO, V_ID_CTR_PROCESSO); 
      ELSE
        DBMS_OUTPUT.PUT_LINE('NÃO POSSO EXECUTAR');  
      END IF;
      
    ELSE
      DBMS_OUTPUT.PUT_LINE('NÃO TEM PROCESSOS PARA EXECUTAR!');  
    END IF;
    
  EXCEPTION
    WHEN ERRO THEN
      P_COD_ERRO := V_ERRO;
      P_MSG_ERRO := V_DES_ERRO;
    WHEN OTHERS THEN
      P_COD_ERRO := SQLCODE;
      P_MSG_ERRO := SQLERRM;    
  END SP_EXECUTA;
  
  /**********************************************************************
  FUNCTION
  FC_EXISTE_PROCESSO.

  **********************************************************************/
  FUNCTION FC_EXISTE_PROCESSO(P_FLG_STATUS VARCHAR2) RETURN BOOLEAN IS    
    --v_qtd number := 0;
  BEGIN
    v_qtd_processo      := 0;
    
    select count(*)
      into v_qtd_processo
      from tsoc_ctr_processo c
     where c.cod_ins = 1
       and c.flg_status = P_FLG_STATUS; 
       
    if v_qtd_processo > 0 then
      return true;
    else
      return false;
    end if; 
     
  EXCEPTION
    WHEN OTHERS THEN
      V_ERRO     := SQLCODE;
      V_DES_ERRO := SQLERRM ||' ERRO NA EXECUÇÃO DA FUNÇÃO: FC_EXISTE_PROCESSO';   
      RAISE ERRO;    
  END FC_EXISTE_PROCESSO;  

  /**********************************************************************
  FUNCTION
  FC_POSSO_EXECUTA.

  **********************************************************************/
  FUNCTION FC_POSSO_EXECUTA(P_COD_INS         OUT NUMBER, 
                            P_ID_PROCESSO     OUT NUMBER, 
                            P_ID_CTR_PROCESSO OUT NUMBER) RETURN BOOLEAN IS  
    CURSOR C_PROCESSO IS
      select c.*, s.qtd_processo
        from tsoc_ctr_processo c, tsoc_par_proc_servidor s, tsoc_par_codigo pc
       where c.cod_ins = s.cod_ins
         and c.id_processo = s.id_processo
         and s.cod_ins = pc.cod_ins
         and s.cod_servidor = pc.cod_par
         and pc.cod_num = 1007 --servidores
         and c.flg_status = 'A' 
         and pc.des_descricao = V_IP_HOST
      order by 1;  

  v_qtd_em_exec integer;
  
  BEGIN
    FOR REG IN C_PROCESSO LOOP
      
      select count(*)
        into v_qtd_em_exec
        from tsoc_ctr_processo c
       where c.cod_ins = reg.cod_ins
         and c.id_processo = reg.id_processo
         and c.flg_status = 'P';
         
      if v_qtd_em_exec < reg.qtd_processo then

        if fc_get_data_execucao(reg.id_processo) = 'S' then
        
          P_COD_INS         := REG.COD_INS;
          P_ID_PROCESSO     := REG.ID_PROCESSO;
          P_ID_CTR_PROCESSO := REG.ID_CTR_PROCESSO;
        
          RETURN TRUE;
        end if;  
      end if;      
           
    END LOOP;  
    
    RETURN FALSE;
           
  EXCEPTION
    WHEN OTHERS THEN
      V_ERRO     := SQLCODE;
      V_DES_ERRO := SQLERRM ||' ERRO NA EXECUÇÃO DA FUNÇÃO: FC_POSSO_EXECUTA';   
      RAISE ERRO;      
  END FC_POSSO_EXECUTA;
  
  /**********************************************************************
  PROCEDURE
  SP_EXEC_PROCESSO.

  **********************************************************************/  
  PROCEDURE SP_EXEC_PROCESSO(P_COD_INS         NUMBER, 
                             P_ID_PROCESSO     NUMBER, 
                             P_ID_CTR_PROCESSO NUMBER) IS
    CURSOR C_PROGRAMA IS
      select g.*
        from tsoc_par_processo p
           , tsoc_par_programa g
       where p.cod_ins = g.cod_ins
         and p.id_programa = g.id_programa
         and p.cod_ins = P_COD_INS
         and p.id_processo = P_ID_PROCESSO
       order by id_processo;
       
  v_sid v$session.sid%type;
  v_pid v$process.spid%type;
         
  BEGIN
    
    select s.sid, p.spid
      into v_sid, v_pid
      from v$session s, v$process p
      where s.paddr = p.addr
      and s.audsid = userenv('sessionid');
      
    FOR REG IN C_PROGRAMA LOOP
            
        --CRIAR UM PROCESSO DE TESTE 28/09/2018
        DBMS_OUTPUT.PUT_LINE('EXECUTANDO: '||REG.NOM_PROGRAMA||
                           ' PARÂMETRO: '||P_ID_CTR_PROCESSO||
                           ' PID: '||v_pid||
                           ' SID: '||v_sid);
    END LOOP; 
    
  EXCEPTION
    WHEN OTHERS THEN
      V_ERRO     := SQLCODE;
      V_DES_ERRO := SQLERRM ||' ERRO NA EXECUÇÃO DO PROCEDIMENTO: SP_EXEC_PROCESSO';   
      RAISE ERRO;     
  END SP_EXEC_PROCESSO;  
  
  /**********************************************************************
  PROCEDURE
  SP_GET_IP_HOST.

  **********************************************************************/
  PROCEDURE SP_GET_IP_HOST IS
    
  BEGIN   

    select UTL_INADDR.GET_HOST_ADDRESS
      into V_IP_HOST
      from dual;
      
  EXCEPTION
    WHEN OTHERS THEN
      V_ERRO     := SQLCODE;
      V_DES_ERRO := SQLERRM ||' ERRO NA EXECUÇÃO DO PROCEDIMENTO: SP_GET_IP_HOST';   
      RAISE ERRO;     
  END SP_GET_IP_HOST;    

  /**********************************************************************
  FUNÇÃO
  FC_GET_DATA_EXECUCAO.

  **********************************************************************/
  FUNCTION FC_GET_DATA_EXECUCAO(P_ID_PROCESSO NUMBER) RETURN VARCHAR2 IS
    V_flg_executa VARCHAR2(1);
  BEGIN   

    select decode(
        case
         --mes
         when p.par_mes = '*' then
           'S'
         else
           case when p.par_mes = s.mes then 
             'S' 
           else
             'N'
           end
        end,'S',
        decode(
        case
         --semana
         when p.par_semana = '*' then
           'S'
         else
           case when p.par_semana = s.semana then 
             'S' 
           else
             'N'
           end
        end,'S',
        decode(
        case
         --dia
         when p.par_dia = '*' then
           'S'
         else
           case when p.par_dia = s.dia then 
             'S' 
           else
             'N'
           end
        end,'S',
        decode(
        case
         --hora
         when p.par_hora = '*' then
           'S'
         else
           case when p.par_hora = s.hora then 
             'S' 
           else
             'N'
           end
        end,'S',
         case
         --minuto
         when p.par_minuto = '*' then
           'S'
         else
           case when p.par_minuto = s.minuto then 
             'S' 
           else
             'N'
           end
        end ,'N'),'N'),'N'),'N') AS flg_executa  
    into V_flg_executa        
    from esocial.tsoc_par_processo p,
       (select to_char(sysdate, 'mm') MES,
               to_char(sysdate, 'd') SEMANA,
               to_char(sysdate, 'dd') DIA,
               to_char(sysdate, 'hh') HORA,
               to_char(sysdate, 'mi') MINUTO
          from dual) s
    where p.id_processo = P_ID_PROCESSO
      and p.flg_status = 'A';

    RETURN V_flg_executa;  

  EXCEPTION
    WHEN OTHERS THEN
      V_ERRO     := SQLCODE;
      V_DES_ERRO := SQLERRM ||' ERRO NA EXECUÇÃO DA FUNÇÃO: FC_GET_DATA_EXECUCAO';   
      RAISE ERRO;       
  END FC_GET_DATA_EXECUCAO;   


END PAC_ESOCIAL_EXECUTA_PROCESSO;
/
