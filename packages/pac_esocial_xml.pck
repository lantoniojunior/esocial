CREATE OR REPLACE PACKAGE pac_esocial_xml IS

  gb_rec_erro esocial.tsoc_ctr_erro_processo%ROWTYPE;

  PROCEDURE sp_seta_processo(p_id_ctr_processo IN esocial.tsoc_ctr_processo.id_ctr_processo%TYPE,
                             p_nom_evento      IN VARCHAR2,
                             p_qtd_registros   IN NUMBER);

  PROCEDURE sp_xml_lote(p_id_evento IN NUMBER,
                        p_id_pk     IN NUMBER,
                        p_header    OUT CLOB,
                        p_trailer   OUT CLOB);

  PROCEDURE sp_xml_generico(p_cod_ins    IN NUMBER,
                            p_nom_evento IN VARCHAR2,
                            p_acao       IN VARCHAR2,
                            p_id         IN NUMBER);

  PROCEDURE sp_xml_2400(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_xml_2405(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_xml_2410(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_xml_2416(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_xml_2420(p_cod_ins IN NUMBER, p_id IN NUMBER);

  PROCEDURE sp_arqs_qualificacao_cadastral;

END pac_esocial_xml;
/
CREATE OR REPLACE PACKAGE BODY pac_esocial_xml IS

  g_acao         VARCHAR2(1);
  g_paisresid    VARCHAR2(3);
  g_tp_beneficio VARCHAR2(4);

  FUNCTION fc_tag(p_nom_registro IN VARCHAR2,
                  p_cod_ins      IN NUMBER,
                  p_nom_evento   IN VARCHAR2,
                  p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS
  
    vxml          VARCHAR2(100);
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
  
  BEGIN
  
    -- identifico se o parametro é para abertura de tag
  
    IF p_abre_fecha = 'A' THEN
    
      -- verifico se há algum atributo a ser atribuido a tag de abertura e o seu respectivo valor
    
      SELECT tet.nom_registro
        INTO vnom_registro
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tet.tip_elemento = 'A'
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND
             tet.num_seq_sql = decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))
         AND tet.nom_registro_pai = p_nom_registro;
    
      vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';
    
    ELSIF p_abre_fecha = 'F' THEN
    
      vxml := '</' || p_nom_registro || '>';
    
    END IF;
  
    RETURN vxml;
  
  EXCEPTION
    WHEN no_data_found THEN
    
      -- caso não exista atributo definido para a tag, apenas a abro
      vxml := '<' || p_nom_registro || '>';
      RETURN vxml;
    
    WHEN OTHERS THEN
    
      raise_application_error(-20001, 'Erro em fc_tag: ' || SQLERRM);
    
  END fc_tag;

  FUNCTION fc_set_valor(p_nom_evento     IN VARCHAR2,
                        p_cod_ins        IN NUMBER,
                        p_xml            IN CLOB,
                        p_valor          VARCHAR2,
                        p_num_seq_coluna NUMBER) RETURN CLOB IS
  
    vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
    nqtd_maxima_registro tsoc_par_estruturas_xml.nom_registro_pai%TYPE;
    vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;
  
    vxml   CLOB;
    vvalor VARCHAR2(100);
  
    --   raise_tam_invalido EXCEPTION;
  BEGIN
  
    vvalor := p_valor;
  
    -- antes de setar o valor no xml, valido a formatacao do campo e o seu tamanho
    -- se estao de acordo com o parametrizado
  
    SELECT tet.nom_registro, tet.nom_registro_pai, tet.tip_elemento
      INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
      FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
     WHERE tev.cod_ins = tet.cod_ins
       AND tev.cod_evento = tet.cod_evento
       AND tev.num_versao_evento = tet.num_versao_evento
       AND tev.cod_ins = p_cod_ins
       AND tev.nom_evento = p_nom_evento
       AND tev.dat_fim_vig IS NULL
       AND (tet.flg_obrigatorio = 'S' OR
           (tet.flg_obrigatorio = 'N' AND
           tet.num_seq_sql = decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))
       AND tet.num_seq_coluna = p_num_seq_coluna;
  
    /*    IF vdsc_formato IS NOT NULL THEN
    
      vvalor := to_char(to_date(vvalor, 'DD/MM/RRRR'), vdsc_formato);
    
    END IF;*/
  
    /*    IF length(vvalor) > nqtd_maxima_registro THEN
      RAISE raise_tam_invalido;
    ELSE*/
  
    -- seto o valor no xml, dentro da tag passada como parametro
  
    IF vtip_elemento = 'A' THEN
    
      vxml := substr(p_xml,
                     1,
                     (instr(p_xml, vnom_registro, 1)) +
                     length(vnom_registro)) || '"' || vvalor || '"' ||
              substr(p_xml,
                     (instr(p_xml, vnom_registro, 1)) +
                     length(vnom_registro) + 1);
    
    ELSE
    
      vxml := substr(p_xml,
                     1,
                     (instr(p_xml,
                            vnom_registro,
                            instr(p_xml, nqtd_maxima_registro))) +
                     length(vnom_registro)) || vvalor ||
              substr(p_xml,
                     (instr(p_xml,
                            vnom_registro,
                            instr(p_xml, nqtd_maxima_registro))) +
                     length(vnom_registro) + 1);
    
    END IF;
    RETURN vxml;
    --    END IF;
  
  EXCEPTION
  
    WHEN no_data_found THEN
      RETURN p_xml;
    
    /*    WHEN raise_tam_invalido THEN
    raise_application_error(-20001,
                            'Tamanho inválido para ' || vnom_registro ||
                            ' - ' || vvalor || '. Máximo ' ||
                            nqtd_maxima_registro || ' posicoes.');*/
    WHEN OTHERS THEN
    
      raise_application_error(-20001, 'Erro em fc_set_valor: ' || SQLERRM);
    
  END fc_set_valor;

  PROCEDURE sp_xml_lote(p_id_evento IN NUMBER,
                        p_id_pk     IN NUMBER,
                        p_header    OUT CLOB,
                        p_trailer   OUT CLOB) IS
  
    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    vcod_evento      tsoc_par_evento.cod_evento%TYPE;
  
    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;
  
    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
  
    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;
  
    v_valores VARCHAR2(100);
  
  BEGIN
  
  
    -- Retorno query para montar cursor do detalhe do XML e informações fixas do header
  
    SELECT --tsx.dsc_sql,
     tev.num_versao_xml, tev.dsc_encoding_xml, tev.num_cnpj_empregador
      INTO --vdsc_sql,
           vnum_versao_xml,
           vdsc_encoding_xml,
           vnum_cnpj_empregador
      FROM tsoc_par_eventos_xml    tev,
           tsoc_par_estruturas_xml tet,
           tsoc_par_sql_xml        tsx
     WHERE tev.cod_ins = tet.cod_ins
       AND tev.cod_evento = tet.cod_evento
       AND tev.num_versao_evento = tet.num_versao_evento
       AND tev.cod_ins = tsx.cod_ins
       AND tev.cod_evento = tsx.cod_evento
       AND tev.num_versao_evento = tsx.num_versao_evento
       AND tsx.num_seq_sql = 1
       AND tev.cod_ins = 1
       AND tev.cod_evento = 1
       AND tev.dat_fim_vig IS NULL
       AND tet.num_seq = 1
       AND tet.flg_sql = 'S'
       AND tet.flg_obrigatorio = 'S';
  
    --  vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);
    SELECT ev.nom_tabela, ev.cod_evento
      INTO vtab_update, vcod_evento
      FROM tsoc_par_evento ev
     WHERE ev.id_evento = p_id_evento;
  
    vdsc_sql := 'SELECT ev.xmlns         "1",
       ev.tip_evento    "2",
       t1.tpinsc       "3",
       t1.nrinsc       "4",
       tr.tip_inscricao "5",
       tr.num_inscricao "6"
  FROM ' || vtab_update || ' t1,
       tsoc_par_transmissor    tr,
       tsoc_par_evento         ev
 WHERE ev.cod_ins = t1.cod_ins
   AND ev.cod_evento = ''' || vcod_evento || '''
   AND t1.id_pk = ' || p_id_pk;
  
    OPEN cur_tag FOR vdsc_sql;
  
    -- atribuo um id referencia ao cursor e defino as colunas da query no cursor
  
    n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
    dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);
  
    FOR x IN 1 .. cur_count LOOP
    
      -- percorro o cursor e defino os valores para cada coluna
    
      dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);
    
    END LOOP;
  
    WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
    
      -- variavel para controlar array de fechamento das tags
      nfechatag := 1;
    
      cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
              vdsc_encoding_xml || '"?>' || chr(13);
    
      -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo
    
      FOR c_tag IN (SELECT tet.nom_registro,
                           tet.nom_registro_pai,
                           tet.tip_elemento,
                           tet.flg_sql,
                           tet.num_seq_sql,
                           tet.num_seq_coluna
                      FROM tsoc_par_eventos_xml    tev,
                           tsoc_par_estruturas_xml tet
                     WHERE tev.cod_ins = tet.cod_ins
                       AND tev.cod_evento = tet.cod_evento
                       AND tev.num_versao_evento = tet.num_versao_evento
                       AND tev.cod_ins = 1
                       AND tev.cod_evento = 1
                       AND tet.tip_elemento IN ('G', 'CG', 'E')
                       AND tev.dat_fim_vig IS NULL
                       AND (tet.flg_obrigatorio = 'S' OR
                           (tet.flg_obrigatorio = 'N' AND
                           tet.num_seq_sql = 1))
                     ORDER BY num_seq ASC) LOOP
      
        -- identifico se é uma tag de grupo (tags que não possuem valores associados, apenas atributos)
        IF c_tag.tip_elemento IN ('G', 'CG') THEN
        
          -- adiciono no array auxiliar para fechamento das tags
        
          afechatag(nfechatag) := c_tag.nom_registro;
        
          -- chamo a func de montar tag, passando parametro de abertura de tag
        
          cxml := cxml ||
                  fc_tag(c_tag.nom_registro, 1, 'envioLoteEventos', 'A') ||
                  chr(13);
        
          nfechatag := nfechatag + 1;
        ELSE
          -- caso seja uma tag de elemento (tags que possuem valor associado)
        
          cxml := cxml ||
                  fc_tag(c_tag.nom_registro, 1, 'envioLoteEventos', 'A');
        
          -- chamo func de montar tag com parametro de fechamento de tag
          cxml := cxml ||
                  fc_tag(c_tag.nom_registro, 1, 'envioLoteEventos', 'F') ||
                  chr(13);
        
        END IF;
      
      END LOOP;
    
      -- cursor para fechamento das tags de grupo
    
      FOR i IN REVERSE 1 .. afechatag.count LOOP
      
        -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
        -- onde devemos fechar a tag
      
        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = 1
           AND tet.tip_elemento IN ('G', 'CG', 'E')
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = 1))
           AND tev.nom_evento = 'envioLoteEventos'
           AND tet.nom_registro_pai = afechatag(i)
           AND num_seq =
               (SELECT MAX(num_seq)
                  FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
                 WHERE tev.cod_ins = tet.cod_ins
                   AND tev.cod_evento = tet.cod_evento
                   AND tev.num_versao_evento = tet.num_versao_evento
                   AND tev.cod_ins = 1
                   AND tev.nom_evento = 'envioLoteEventos'
                   AND tet.tip_elemento IN ('G', 'CG', 'E')
                   AND tev.dat_fim_vig IS NULL
                   AND (tet.flg_obrigatorio = 'S' OR
                       (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = 1))
                   AND tet.nom_registro_pai = afechatag(i));
      
        -- identifico o ponto onde deverá ser fechado a tag e chamo a func passando parametro de fechamento
        cxml := substr(cxml,
                       1,
                       (instr(cxml, vnom_registro, -1)) +
                       length(vnom_registro) + 1) ||
                fc_tag(afechatag(i), 1, 'envioLoteEventos', 'F') ||
                substr(cxml,
                       (instr(cxml, vnom_registro, -1)) +
                       length(vnom_registro) + 1);
      
      END LOOP;
    
      FOR x IN 1 .. cur_count LOOP
      
        -- seta no xml os valores retornados pelo cursor parametrizado
      
        dbms_sql.column_value(n_cursor_control, x, v_valores);
        cxml := fc_set_valor('envioLoteEventos',
                             1,
                             cxml,
                             v_valores,
                             to_number(cur_desc(x).col_name));
      END LOOP;
    
      /*    
      EXECUTE IMMEDIATE 'UPDATE ' || vtab_update || ' SET XML_ENVIO = ''' || cxml ||
                        ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' || p_id;
      COMMIT;*/
    
      cxml := REPLACE(cxml, '_', '');
    
      p_header := substr(cxml, 1, (instr(cxml, '<evento ')) - 1);
    
      p_trailer := substr(cxml, (instr(cxml, '</eventos>')));
    
--      dbms_output.put_line(cxml);
    
    END LOOP;
  
    dbms_sql.close_cursor(n_cursor_control);
  
  END sp_xml_lote;

  PROCEDURE sp_gera_erro_processo IS
    v_id_cad_erro esocial.tsoc_ctr_erro_processo.id_erro%TYPE;
  BEGIN
  
    v_id_cad_erro := esocial.esoc_seq_id_erro_processo.nextval;
  
    INSERT INTO esocial.tsoc_ctr_erro_processo
      (id_erro,
       cod_ins,
       id_cad,
       nom_processo,
       tipo_evento,
       desc_erro,
       dat_ing,
       dat_ult_atu,
       nom_usu_ult_atu,
       nom_pro_ult_atu,
       desc_erro_bd,
       num_processo,
       des_identificador,
       flg_tipo_erro,
       id_ctr_processo)
    VALUES
      (v_id_cad_erro,
       gb_rec_erro.cod_ins,
       gb_rec_erro.id_cad,
       gb_rec_erro.nom_processo,
       gb_rec_erro.tipo_evento,
       gb_rec_erro.desc_erro,
       SYSDATE,
       SYSDATE,
       'ESOCIAL',
       'SP_GERA_ERRO_PROCESSO',
       gb_rec_erro.desc_erro_bd,
       gb_rec_erro.num_processo,
       gb_rec_erro.des_identificador,
       gb_rec_erro.flg_tipo_erro,
       gb_rec_erro.id_ctr_processo);
  
    COMMIT;
  
  END sp_gera_erro_processo;

  PROCEDURE sp_seta_processo(p_id_ctr_processo IN esocial.tsoc_ctr_processo.id_ctr_processo%TYPE,
                             p_nom_evento      IN VARCHAR2,
                             p_qtd_registros   IN NUMBER) IS
  BEGIN
  
    IF p_nom_evento = 'INICIO_PROCESSAMENTO' THEN
    
      UPDATE esocial.tsoc_ctr_processo
         SET dat_inicio      = SYSDATE,
             dat_fim         = NULL,
             flg_status      = 'P',
             dat_ult_atu     = SYSDATE,
             nom_usu_ult_atu = 'ESOCIAL',
             nom_pro_ult_atu = 'SP_SETA_PROCESSO'
       WHERE id_ctr_processo = p_id_ctr_processo;
    
      COMMIT;
    
    ELSIF p_nom_evento = 'FIM_PROCESSAMENTO' THEN
    
      UPDATE esocial.tsoc_ctr_processo
         SET dat_fim         = SYSDATE,
             flg_status      = 'F',
             qtd_registros   = p_qtd_registros,
             dat_ult_atu     = SYSDATE,
             nom_usu_ult_atu = 'ESOCIAL',
             nom_pro_ult_atu = 'SP_SETA_PROCESSO'
       WHERE id_ctr_processo = p_id_ctr_processo;
    
      COMMIT;
    
    ELSIF p_nom_evento = 'ATUALIZA_QUANTIDADE' THEN
    
      --ATUALIZACAO DE QUANTIDADE DE REGISTROS
      UPDATE esocial.tsoc_ctr_processo
         SET qtd_registros   = p_qtd_registros,
             dat_ult_atu     = SYSDATE,
             nom_usu_ult_atu = 'ESOCIAL',
             nom_pro_ult_atu = 'SP_SETA_PROCESSO'
       WHERE id_ctr_processo = p_id_ctr_processo;
    
      COMMIT;
    
    ELSE
    
      --ERRO NO PROCESSAMENTO
      UPDATE esocial.tsoc_ctr_processo
         SET flg_status      = 'E',
             dat_ult_atu     = SYSDATE,
             nom_usu_ult_atu = 'ESOCIAL',
             nom_pro_ult_atu = 'SP_SETA_PROCESSO'
       WHERE id_ctr_processo = p_id_ctr_processo;
    
      COMMIT;
    
    END IF;
  
  END sp_seta_processo;

  PROCEDURE sp_xml_generico(p_cod_ins    IN NUMBER,
                            p_nom_evento IN VARCHAR2,
                            p_acao       IN VARCHAR2,
                            p_id         IN NUMBER) IS
  
    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
  
    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;
  
    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    --    vsysdate      VARCHAR2(14) := to_char(SYSDATE, 'RRRRMMDDHH24MISS');
    --    vid           VARCHAR2(36);
    --    nid           NUMBER := 1;
    vdata_ini DATE;
    vdata_fim DATE;
    --    vcod_rubr     NUMBER;
  
    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;
  
    v_valores VARCHAR2(100);
  
  BEGIN
    g_acao := upper(p_acao);
  
    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';
  
    -- Retorno query para montar cursor do detalhe do XML e informações fixas do header
  
    SELECT tsx.dsc_sql,
           tev.num_versao_xml,
           tev.dsc_encoding_xml,
           tev.num_cnpj_empregador
      INTO vdsc_sql,
           vnum_versao_xml,
           vdsc_encoding_xml,
           vnum_cnpj_empregador
      FROM tsoc_par_eventos_xml    tev,
           tsoc_par_estruturas_xml tet,
           tsoc_par_sql_xml        tsx
     WHERE tev.cod_ins = tet.cod_ins
       AND tev.cod_evento = tet.cod_evento
       AND tev.num_versao_evento = tet.num_versao_evento
       AND tev.cod_ins = tsx.cod_ins
       AND tev.cod_evento = tsx.cod_evento
       AND tev.num_versao_evento = tsx.num_versao_evento
       AND tsx.num_seq_sql = decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)
       AND tev.cod_ins = p_cod_ins
       AND tev.nom_evento = p_nom_evento
       AND tev.dat_fim_vig IS NULL
       AND tet.num_seq = 1
       AND tet.flg_sql = 'S'
       AND tet.flg_obrigatorio = 'S';
  
    --  vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);
  
    --    vdsc_sql := vdsc_sql || ' WHERE id_pk = ' || p_id;
  
    OPEN cur_tag FOR vdsc_sql;
  
    -- atribuo um id referencia ao cursor e defino as colunas da query no cursor
  
    n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
    dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);
  
    FOR x IN 1 .. cur_count LOOP
    
      -- percorro o cursor e defino os valores para cada coluna
    
      dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);
    
    END LOOP;
  
    WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
    
      vdata_ini := SYSDATE;
      -- variavel para controlar array de fechamento das tags
      nfechatag := 1;
    
/*      cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
              vdsc_encoding_xml || '"?>' || chr(13);*/
    
      -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo
    
      FOR c_tag IN (SELECT tet.nom_registro,
                           tet.nom_registro_pai,
                           tet.tip_elemento,
                           tet.flg_sql,
                           tet.num_seq_sql,
                           tet.num_seq_coluna
                      FROM tsoc_par_eventos_xml    tev,
                           tsoc_par_estruturas_xml tet
                     WHERE tev.cod_ins = tet.cod_ins
                       AND tev.cod_evento = tet.cod_evento
                       AND tev.num_versao_evento = tet.num_versao_evento
                       AND tev.cod_ins = p_cod_ins
                       AND tev.nom_evento = p_nom_evento
                       AND tet.tip_elemento IN ('G', 'CG', 'E')
                       AND tev.dat_fim_vig IS NULL
                       AND (tet.flg_obrigatorio = 'S' OR
                           (tet.flg_obrigatorio = 'N' AND
                           tet.num_seq_sql =
                           decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))
                     ORDER BY num_seq ASC) LOOP
      
        -- identifico se é uma tag de grupo (tags que não possuem valores associados, apenas atributos)
        IF c_tag.tip_elemento IN ('G', 'CG') THEN
        
          -- adiciono no array auxiliar para fechamento das tags
        
          afechatag(nfechatag) := c_tag.nom_registro;
        
          -- chamo a func de montar tag, passando parametro de abertura de tag
        
          cxml := cxml ||
                  fc_tag(c_tag.nom_registro, p_cod_ins, p_nom_evento, 'A') ||
                  chr(13);
        
          nfechatag := nfechatag + 1;
        ELSE
          -- caso seja uma tag de elemento (tags que possuem valor associado)
        
          cxml := cxml ||
                  fc_tag(c_tag.nom_registro, p_cod_ins, p_nom_evento, 'A');
        
          -- chamo func de montar tag com parametro de fechamento de tag
          cxml := cxml ||
                  fc_tag(c_tag.nom_registro, p_cod_ins, p_nom_evento, 'F') ||
                  chr(13);
        
        END IF;
      
      END LOOP;
    
      -- defino o valor a ser setado no atributo ID do xml 
    
      --      vid := 'ID1' || vnum_cnpj_empregador || vsysdate || lpad(nid, 5, '0');
    
      --      cxml := fc_set_id(cxml, vid);
    
      -- sequencial do id
      --      nid := nid + 1;
    
      -- cursor para fechamento das tags de grupo
    
      FOR i IN REVERSE 1 .. afechatag.count LOOP
      
        -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
        -- onde devemos fechar a tag
      
        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tet.tip_elemento IN ('G', 'CG', 'E')
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND
               tet.num_seq_sql =
               decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))
           AND tev.nom_evento = p_nom_evento
           AND tet.nom_registro_pai = afechatag(i)
           AND num_seq =
               (SELECT MAX(num_seq)
                  FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
                 WHERE tev.cod_ins = tet.cod_ins
                   AND tev.cod_evento = tet.cod_evento
                   AND tev.num_versao_evento = tet.num_versao_evento
                   AND tev.cod_ins = p_cod_ins
                   AND tev.nom_evento = p_nom_evento
                   AND tet.tip_elemento IN ('G', 'CG', 'E')
                   AND tev.dat_fim_vig IS NULL
                   AND (tet.flg_obrigatorio = 'S' OR
                       (tet.flg_obrigatorio = 'N' AND
                       tet.num_seq_sql =
                       decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))
                   AND tet.nom_registro_pai = afechatag(i));
      
        -- identifico o ponto onde deverá ser fechado a tag e chamo a func passando parametro de fechamento
        cxml := substr(cxml,
                       1,
                       (instr(cxml, vnom_registro, -1)) +
                       length(vnom_registro) + 1) ||
                fc_tag(afechatag(i), p_cod_ins, p_nom_evento, 'F') ||
                substr(cxml,
                       (instr(cxml, vnom_registro, -1)) +
                       length(vnom_registro) + 1);
      
      END LOOP;
    
      FOR x IN 1 .. cur_count LOOP
      
        -- seta no xml os valores retornados pelo cursor parametrizado
      
        dbms_sql.column_value(n_cursor_control, x, v_valores);
        cxml := fc_set_valor(p_nom_evento,
                             p_cod_ins,
                             cxml,
                             v_valores,
                             to_number(cur_desc(x).col_name));
      
      /*        IF x = 8 THEN
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            vcod_rubr := v_valores;
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          END IF;*/
      
      END LOOP;
    
      vdata_fim := SYSDATE;
    
      /*      UPDATE tsoc_1010_rubrica r
        SET r.xml_envio        = cxml,
            r.ctr_dat_ini_proc = vdata_ini,
            r.ctr_dat_fim_proc = vdata_fim
      WHERE r.rowid = v_valores;*/
    
      /*      INSERT INTO tsoc_analise_tempo_geracao atg
        (cod_rubrica, xml_proc, dat_ini_proc, dat_fim_proc)
      VALUES
        (vcod_rubr, cxml, vdata_ini, vdata_fim);
      
      COMMIT;*/
      /*    
      EXECUTE IMMEDIATE 'UPDATE ' || vtab_update || ' SET XML_ENVIO = ''' || cxml ||
                        ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' || p_id;
      COMMIT;*/
    
      dbms_output.put_line(cxml);
    
    END LOOP;
  
    dbms_sql.close_cursor(n_cursor_control);
  
    /* EXCEPTION
    WHEN OTHERS THEN
    
      raise_application_error(-20001,
                              'Erro em sp_xml_generico: ' || SQLERRM);*/
  
  END sp_xml_generico;

  PROCEDURE sp_xml_2400(p_id_ctr_processo IN NUMBER) IS
  
    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;
  
    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;
  
    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    --    vdata_ini     DATE;
    --    vdata_fim     DATE;
  
    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;
  
    v_valores VARCHAR2(100);
  
    FUNCTION fc_set_valor_2400(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS
    
      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;
    
      vxml   CLOB;
      vvalor VARCHAR2(100);
    
      --      raise_tam_invalido EXCEPTION;
    BEGIN
    
      vvalor := p_valor;
    
      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND
             tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))
            
         AND tet.num_seq_coluna = p_num_seq_coluna;
    
      -- seto o valor no xml, dentro da tag passada como parametro
    
      IF vtip_elemento = 'A' THEN
      
        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);
      
      ELSE
      
        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);
      
      END IF;
      RETURN vxml;
    
    EXCEPTION
    
      WHEN no_data_found THEN
        RETURN p_xml;
      
      WHEN OTHERS THEN
      
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_2400';
        gb_rec_erro.tipo_evento       := '2400';
        gb_rec_erro.desc_erro         := 'ERRO NA FUNÇÃO FC_SET_VALOR_2400';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        sp_gera_erro_processo;
      
    END fc_set_valor_2400;
  
    FUNCTION fc_tag_2400(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS
    
      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    
    BEGIN
    
      -- identifico se o parametro é para abertura de tag
    
      IF p_abre_fecha = 'A' THEN
      
        -- verifico se há algum atributo a ser atribuido a tag de abertura e o seu respectivo valor
      
        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND
               tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))
              
           AND tet.nom_registro_pai = p_nom_registro;
      
        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';
      
      ELSIF p_abre_fecha = 'F' THEN
      
        vxml := '</' || p_nom_registro || '>';
      
      END IF;
    
      RETURN vxml;
    
    EXCEPTION
      WHEN no_data_found THEN
      
        -- caso não exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;
      
      WHEN OTHERS THEN
      
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_2400';
        gb_rec_erro.tipo_evento       := '2400';
        gb_rec_erro.desc_erro         := 'ERRO NA FUNÇÃO FC_TAG_2400';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        sp_gera_erro_processo;
      
    END fc_tag_2400;
  
  BEGIN
  
    gb_rec_erro.num_processo := esocial.esoc_seq_num_processo.nextval;
  
    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;
  
    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';
  
    -- Retorno query para montar cursor do detalhe do XML e informações fixas do header
  
    BEGIN
    
      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';
    
    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_2400';
        gb_rec_erro.tipo_evento       := '2400';
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;
  
    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);
  
    FOR x IN (SELECT id_pk, endereco_paisresid
                FROM tsoc_2400_beneficiario_ini
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX') LOOP
    
      g_paisresid := x.endereco_paisresid;
    
      afechatag.delete();
    
      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = decode(g_paisresid, '105', 1, 2)
            
         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtCdBenefIn'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';
    
      vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);
    
      vdsc_sql := vdsc_sql || ' WHERE id_pk = ' || x.id_pk;
    
      OPEN cur_tag FOR vdsc_sql;
    
      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor
    
      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);
    
      FOR x IN 1 .. cur_count LOOP
      
        -- percorro o cursor e defino os valores para cada coluna
      
        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);
      
      END LOOP;
    
      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
        
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;
        
/*          cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
                  vdsc_encoding_xml || '"?>' || chr(13);*/
        
          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo
        
          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtCdBenefIn'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND
                               tet.num_seq_sql =
                               decode(g_paisresid, '105', 1, 2)))
                        
                         ORDER BY num_seq ASC) LOOP
          
            -- identifico se é uma tag de grupo (tags que não possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN
            
              -- adiciono no array auxiliar para fechamento das tags
            
              afechatag(nfechatag) := c_tag.nom_registro;
            
              -- chamo a func de montar tag, passando parametro de abertura de tag
            
              cxml := cxml || fc_tag_2400(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefIn',
                                          'A') || chr(13);
            
              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)
            
              cxml := cxml || fc_tag_2400(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefIn',
                                          'A');
            
              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_2400(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefIn',
                                          'F') || chr(13);
            
            END IF;
          
          END LOOP;
        
          -- cursor para fechamento das tags de grupo
        
          FOR i IN REVERSE 1 .. afechatag.count LOOP
          
            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN
            
              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND
                     tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))
                    
                 AND tev.nom_evento = 'evtCdBenefIn'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtCdBenefIn'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND
                             tet.num_seq_sql =
                             decode(g_paisresid, '105', 1, 2)))
                            
                         AND tet.nom_registro_pai = afechatag(i));
            
            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_2400';
                gb_rec_erro.tipo_evento       := '2400';
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 2400';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde deverá ser fechado a tag e chamo a func passando parametro de fechamento
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_2400(afechatag(i),
                                v_cod_ins,
                                'evtCdBenefIn',
                                'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);
          
          END LOOP;
        
          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado
          
            dbms_sql.column_value(n_cursor_control, x, v_valores);
            cxml := fc_set_valor_2400('evtCdBenefIn',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));
          
          END LOOP;
        
          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;
        
          --              dbms_output.put_line(cxml);
        
          v_qtd_registros := v_qtd_registros + 1;
        
          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_2400';
            gb_rec_erro.tipo_evento       := '2400';
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAÇÃO DO XML 2400';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            sp_gera_erro_processo;
          
            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;
          
        END;
      
      END LOOP;
    
      dbms_sql.close_cursor(n_cursor_control);
    
      v_faixa_ini := v_faixa_ini + 1;
    
    END LOOP;
  
    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);
  
  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_2400';
      gb_rec_erro.tipo_evento       := '2400';
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAÇÃO DO XML 2400';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);
    
  END sp_xml_2400;

  PROCEDURE sp_xml_2405(p_id_ctr_processo IN NUMBER) IS
  
    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;
  
    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;
  
    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    -- vdata_ini     DATE;
    -- vdata_fim     DATE;
  
    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;
  
    v_valores VARCHAR2(100);
  
    FUNCTION fc_set_valor_2405(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS
    
      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;
    
      vxml   CLOB;
      vvalor VARCHAR2(100);
    
      --      raise_tam_invalido EXCEPTION;
    BEGIN
    
      vvalor := p_valor;
    
      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND
             tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))
            
         AND tet.num_seq_coluna = p_num_seq_coluna;
    
      -- seto o valor no xml, dentro da tag passada como parametro
    
      IF vtip_elemento = 'A' THEN
      
        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);
      
      ELSE
      
        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);
      
      END IF;
      RETURN vxml;
    
    EXCEPTION
    
      WHEN no_data_found THEN
        RETURN p_xml;
      
      WHEN OTHERS THEN
      
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_2405';
        gb_rec_erro.tipo_evento       := '2405';
        gb_rec_erro.desc_erro         := 'ERRO NA FUNÇÃO FC_SET_VALOR_2405';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        sp_gera_erro_processo;
      
    END fc_set_valor_2405;
  
    FUNCTION fc_tag_2405(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS
    
      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    
    BEGIN
    
      -- identifico se o parametro é para abertura de tag
    
      IF p_abre_fecha = 'A' THEN
      
        -- verifico se há algum atributo a ser atribuido a tag de abertura e o seu respectivo valor
      
        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND
               tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))
              
           AND tet.nom_registro_pai = p_nom_registro;
      
        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';
      
      ELSIF p_abre_fecha = 'F' THEN
      
        vxml := '</' || p_nom_registro || '>';
      
      END IF;
    
      RETURN vxml;
    
    EXCEPTION
      WHEN no_data_found THEN
      
        -- caso não exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;
      
      WHEN OTHERS THEN
      
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_2405';
        gb_rec_erro.tipo_evento       := '2405';
        gb_rec_erro.desc_erro         := 'ERRO NA FUNÇÃO FC_TAG_2405';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        sp_gera_erro_processo;
      
    END fc_tag_2405;
  
  BEGIN
  
    gb_rec_erro.num_processo := esocial.esoc_seq_num_processo.nextval;
  
    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;
  
    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';
  
    -- Retorno query para montar cursor do detalhe do XML e informações fixas do header
  
    BEGIN
    
      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';
    
    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_2405';
        gb_rec_erro.tipo_evento       := '2405';
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;
  
    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);
  
    FOR x IN (SELECT id_pk, endereco_paisresid
                FROM tsoc_2405_beneficiario_alt
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX') LOOP
    
      g_paisresid := x.endereco_paisresid;
    
      afechatag.delete();
    
      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = decode(g_paisresid, '105', 1, 2)
            
         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtCdBenefAlt'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';
    
      vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);
    
      vdsc_sql := vdsc_sql || ' WHERE id_pk = ' || x.id_pk;
    
      OPEN cur_tag FOR vdsc_sql;
    
      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor
    
      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);
    
      FOR x IN 1 .. cur_count LOOP
      
        -- percorro o cursor e defino os valores para cada coluna
      
        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);
      
      END LOOP;
    
      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
        
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;
        
/*          cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
                  vdsc_encoding_xml || '"?>' || chr(13);*/
        
          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo
        
          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtCdBenefAlt'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND
                               tet.num_seq_sql =
                               decode(g_paisresid, '105', 1, 2)))
                        
                         ORDER BY num_seq ASC) LOOP
          
            -- identifico se é uma tag de grupo (tags que não possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN
            
              -- adiciono no array auxiliar para fechamento das tags
            
              afechatag(nfechatag) := c_tag.nom_registro;
            
              -- chamo a func de montar tag, passando parametro de abertura de tag
            
              cxml := cxml || fc_tag_2405(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefAlt',
                                          'A') || chr(13);
            
              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)
            
              cxml := cxml || fc_tag_2405(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefAlt',
                                          'A');
            
              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_2405(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefAlt',
                                          'F') || chr(13);
            
            END IF;
          
          END LOOP;
        
          -- cursor para fechamento das tags de grupo
        
          FOR i IN REVERSE 1 .. afechatag.count LOOP
          
            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN
            
              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND
                     tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))
                    
                 AND tev.nom_evento = 'evtCdBenefAlt'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtCdBenefAlt'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND
                             tet.num_seq_sql =
                             decode(g_paisresid, '105', 1, 2)))
                            
                         AND tet.nom_registro_pai = afechatag(i));
            
            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_2405';
                gb_rec_erro.tipo_evento       := '2405';
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 2405';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde deverá ser fechado a tag e chamo a func passando parametro de fechamento
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_2405(afechatag(i),
                                v_cod_ins,
                                'evtCdBenefAlt',
                                'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);
          
          END LOOP;
        
          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado
          
            dbms_sql.column_value(n_cursor_control, x, v_valores);
            cxml := fc_set_valor_2405('evtCdBenefAlt',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));
          
          END LOOP;
        
          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;
        
          --      dbms_output.put_line(cxml);
        
          v_qtd_registros := v_qtd_registros + 1;
        
          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_2405';
            gb_rec_erro.tipo_evento       := '2405';
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAÇÃO DO XML 2405';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            sp_gera_erro_processo;
          
            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;
          
        END;
      
      END LOOP;
    
      dbms_sql.close_cursor(n_cursor_control);
    
      v_faixa_ini := v_faixa_ini + 1;
    
    END LOOP;
  
    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);
  
  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_2405';
      gb_rec_erro.tipo_evento       := '2405';
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAÇÃO DO XML 2405';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);
    
  END sp_xml_2405;

  PROCEDURE sp_xml_2410(p_id_ctr_processo IN NUMBER) IS
  
    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;
  
    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;
  
    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    --    vdata_ini     DATE;
    --  vdata_fim     DATE;
  
    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;
  
    v_valores VARCHAR2(100);
  
    FUNCTION fc_set_valor_2410(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS
    
      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;
    
      vxml   CLOB;
      vvalor VARCHAR2(100);
    
      --      raise_tam_invalido EXCEPTION;
    BEGIN
    
      vvalor := p_valor;
    
      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
               WHEN g_tp_beneficio IN
                    (SELECT DISTINCT cod_esocial
                       FROM esocial.tsoc_par_sigeprev_esocial cse
                      WHERE cse.cod_tipo = 7
                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                2
               ELSE
                1
             END))
         AND tet.num_seq_coluna = p_num_seq_coluna;
    
      -- seto o valor no xml, dentro da tag passada como parametro
    
      IF vtip_elemento = 'A' THEN
      
        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);
      
      ELSE
      
        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);
      
      END IF;
      RETURN vxml;
    
    EXCEPTION
    
      WHEN no_data_found THEN
        RETURN p_xml;
      
      WHEN OTHERS THEN
      
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_2410';
        gb_rec_erro.tipo_evento       := '2410';
        gb_rec_erro.desc_erro         := 'ERRO NA FUNÇÃO FC_SET_VALOR_2410';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        sp_gera_erro_processo;
      
    END fc_set_valor_2410;
  
    FUNCTION fc_tag_2410(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS
    
      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    
    BEGIN
    
      -- identifico se o parametro é para abertura de tag
    
      IF p_abre_fecha = 'A' THEN
      
        -- verifico se há algum atributo a ser atribuido a tag de abertura e o seu respectivo valor
      
        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                 WHEN g_tp_beneficio IN
                      (SELECT DISTINCT cod_esocial
                         FROM esocial.tsoc_par_sigeprev_esocial cse
                        WHERE cse.cod_tipo = 7
                          AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                  2
                 ELSE
                  1
               END))
           AND tet.nom_registro_pai = p_nom_registro;
      
        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';
      
      ELSIF p_abre_fecha = 'F' THEN
      
        vxml := '</' || p_nom_registro || '>';
      
      END IF;
    
      RETURN vxml;
    
    EXCEPTION
      WHEN no_data_found THEN
      
        -- caso não exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;
      
      WHEN OTHERS THEN
      
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_2410';
        gb_rec_erro.tipo_evento       := '2410';
        gb_rec_erro.desc_erro         := 'ERRO NA FUNÇÃO FC_TAG_2410';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        sp_gera_erro_processo;
      
    END fc_tag_2410;
  
  BEGIN
  
    gb_rec_erro.num_processo := esocial.esoc_seq_num_processo.nextval;
  
    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;
  
    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';
  
    -- Retorno query para montar cursor do detalhe do XML e informações fixas do header
  
    BEGIN
    
      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';
    
    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_2410';
        gb_rec_erro.tipo_evento       := '2410';
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;
  
    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);
  
    FOR x IN (SELECT id_pk, dadosbeneficio_tpbeneficio
                FROM tsoc_2410_beneficio_ini
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX') LOOP
    
      g_tp_beneficio := x.dadosbeneficio_tpbeneficio;
    
      afechatag.delete();
    
      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = CASE
               WHEN g_tp_beneficio IN
                    (SELECT DISTINCT cod_esocial
                       FROM esocial.tsoc_par_sigeprev_esocial cse
                      WHERE cse.cod_tipo = 7
                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                2
               ELSE
                1
             END
         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtCdBenIn'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';
    
      vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);
    
      vdsc_sql := vdsc_sql || ' WHERE id_pk = ' || x.id_pk;
    
      OPEN cur_tag FOR vdsc_sql;
    
      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor
    
      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);
    
      FOR x IN 1 .. cur_count LOOP
      
        -- percorro o cursor e defino os valores para cada coluna
      
        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);
      
      END LOOP;
    
      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
        
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;
        
/*          cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
                  vdsc_encoding_xml || '"?>' || chr(13);*/
        
          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo
        
          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet. tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtCdBenIn'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                                 WHEN g_tp_beneficio IN
                                      (SELECT DISTINCT cod_esocial
                                         FROM esocial.tsoc_par_sigeprev_esocial cse
                                        WHERE cse.cod_tipo = 7
                                          AND upper(des_esocial) LIKE
                                              '%PENS_O%MORTE%') THEN
                                  2
                                 ELSE
                                  1
                               END))
                         ORDER BY num_seq ASC) LOOP
          
            -- identifico se é uma tag de grupo (tags que não possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN
            
              -- adiciono no array auxiliar para fechamento das tags
            
              afechatag(nfechatag) := c_tag.nom_registro;
            
              -- chamo a func de montar tag, passando parametro de abertura de tag
            
              cxml := cxml || fc_tag_2410(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenIn',
                                          'A') || chr(13);
            
              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)
            
              cxml := cxml || fc_tag_2410(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenIn',
                                          'A');
            
              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_2410(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenIn',
                                          'F') || chr(13);
            
            END IF;
          
          END LOOP;
        
          -- cursor para fechamento das tags de grupo
        
          FOR i IN REVERSE 1 .. afechatag.count LOOP
          
            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN
            
              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                       WHEN g_tp_beneficio IN
                            (SELECT DISTINCT cod_esocial
                               FROM esocial.tsoc_par_sigeprev_esocial cse
                              WHERE cse.cod_tipo = 7
                                AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                        2
                       ELSE
                        1
                     END))
                 AND tev.nom_evento = 'evtCdBenIn'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtCdBenIn'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                               WHEN g_tp_beneficio IN
                                    (SELECT DISTINCT cod_esocial
                                       FROM esocial.tsoc_par_sigeprev_esocial cse
                                      WHERE cse.cod_tipo = 7
                                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                                2
                               ELSE
                                1
                             END))
                         AND tet.nom_registro_pai = afechatag(i));
            
            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_2410';
                gb_rec_erro.tipo_evento       := '2410';
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 2410';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde deverá ser fechado a tag e chamo a func passando parametro de fechamento
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_2410(afechatag(i), v_cod_ins, 'evtCdBenIn', 'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);
          
          END LOOP;
        
          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado
          
            dbms_sql.column_value(n_cursor_control, x, v_valores);
            cxml := fc_set_valor_2410('evtCdBenIn',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));
          
          END LOOP;
        
          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;
        
          --  dbms_output.put_line(cxml);
        
          v_qtd_registros := v_qtd_registros + 1;
        
          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_2410';
            gb_rec_erro.tipo_evento       := '2410';
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAÇÃO DO XML 2410';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            sp_gera_erro_processo;
          
            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;
          
        END;
      
      END LOOP;
    
      dbms_sql.close_cursor(n_cursor_control);
    
      v_faixa_ini := v_faixa_ini + 1;
    
    END LOOP;
  
    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);
  
  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_2410';
      gb_rec_erro.tipo_evento       := '2410';
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAÇÃO DO XML 2410';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);
    
  END sp_xml_2410;

  PROCEDURE sp_xml_2416(p_id_ctr_processo IN NUMBER) IS
  
    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;
  
    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;
  
    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    -- vdata_ini     DATE;
    -- vdata_fim     DATE;
  
    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;
  
    v_valores VARCHAR2(100);
  
    FUNCTION fc_set_valor_2416(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS
    
      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;
    
      vxml   CLOB;
      vvalor VARCHAR2(100);
    
      --      raise_tam_invalido EXCEPTION;
    BEGIN
    
      vvalor := p_valor;
    
      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
               WHEN g_tp_beneficio IN
                    (SELECT DISTINCT cod_esocial
                       FROM esocial.tsoc_par_sigeprev_esocial cse
                      WHERE cse.cod_tipo = 7
                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                2
               ELSE
                1
             END))
         AND tet.num_seq_coluna = p_num_seq_coluna;
    
      -- seto o valor no xml, dentro da tag passada como parametro
    
      IF vtip_elemento = 'A' THEN
      
        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);
      
      ELSE
      
        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);
      
      END IF;
      RETURN vxml;
    
    EXCEPTION
    
      WHEN no_data_found THEN
        RETURN p_xml;
      
      WHEN OTHERS THEN
      
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_2416';
        gb_rec_erro.tipo_evento       := '2416';
        gb_rec_erro.desc_erro         := 'ERRO NA FUNÇÃO FC_SET_VALOR_2416';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        sp_gera_erro_processo;
      
    END fc_set_valor_2416;
  
    FUNCTION fc_tag_2416(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS
    
      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    
    BEGIN
    
      -- identifico se o parametro é para abertura de tag
    
      IF p_abre_fecha = 'A' THEN
      
        -- verifico se há algum atributo a ser atribuido a tag de abertura e o seu respectivo valor
      
        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                 WHEN g_tp_beneficio IN
                      (SELECT DISTINCT cod_esocial
                         FROM esocial.tsoc_par_sigeprev_esocial cse
                        WHERE cse.cod_tipo = 7
                          AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                  2
                 ELSE
                  1
               END))
           AND tet.nom_registro_pai = p_nom_registro;
      
        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';
      
      ELSIF p_abre_fecha = 'F' THEN
      
        vxml := '</' || p_nom_registro || '>';
      
      END IF;
    
      RETURN vxml;
    
    EXCEPTION
      WHEN no_data_found THEN
      
        -- caso não exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;
      
      WHEN OTHERS THEN
      
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_2416';
        gb_rec_erro.tipo_evento       := '2416';
        gb_rec_erro.desc_erro         := 'ERRO NA FUNÇÃO FC_TAG_2416';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        sp_gera_erro_processo;
      
    END fc_tag_2416;
  
  BEGIN
  
    gb_rec_erro.num_processo := esocial.esoc_seq_num_processo.nextval;
  
    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;
  
    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';
  
    -- Retorno query para montar cursor do detalhe do XML e informações fixas do header
  
    BEGIN
    
      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';
    
    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_2416';
        gb_rec_erro.tipo_evento       := '2416';
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;
  
    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);
  
    FOR x IN (SELECT id_pk, dadosbeneficio_tpbeneficio
                FROM tsoc_2416_beneficio_alt
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX') LOOP
    
      g_tp_beneficio := x.dadosbeneficio_tpbeneficio;
    
      afechatag.delete();
    
      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = CASE
               WHEN g_tp_beneficio IN
                    (SELECT DISTINCT cod_esocial
                       FROM esocial.tsoc_par_sigeprev_esocial cse
                      WHERE cse.cod_tipo = 7
                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                2
               ELSE
                1
             END
         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtCdBenAlt'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';
    
      vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);
    
      vdsc_sql := vdsc_sql || ' WHERE id_pk = ' || x.id_pk;
    
      OPEN cur_tag FOR vdsc_sql;
    
      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor
    
      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);
    
      FOR x IN 1 .. cur_count LOOP
      
        -- percorro o cursor e defino os valores para cada coluna
      
        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);
      
      END LOOP;
    
      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
        
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;
        
/*          cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
                  vdsc_encoding_xml || '"?>' || chr(13);*/
        
          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo
        
          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtCdBenAlt'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                                 WHEN g_tp_beneficio IN
                                      (SELECT DISTINCT cod_esocial
                                         FROM esocial.tsoc_par_sigeprev_esocial cse
                                        WHERE cse.cod_tipo = 7
                                          AND upper(des_esocial) LIKE
                                              '%PENS_O%MORTE%') THEN
                                  2
                                 ELSE
                                  1
                               END))
                         ORDER BY num_seq ASC) LOOP
          
            -- identifico se é uma tag de grupo (tags que não possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN
            
              -- adiciono no array auxiliar para fechamento das tags
            
              afechatag(nfechatag) := c_tag.nom_registro;
            
              -- chamo a func de montar tag, passando parametro de abertura de tag
            
              cxml := cxml || fc_tag_2416(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenAlt',
                                          'A') || chr(13);
            
              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)
            
              cxml := cxml || fc_tag_2416(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenAlt',
                                          'A');
            
              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_2416(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenAlt',
                                          'F') || chr(13);
            
            END IF;
          
          END LOOP;
        
          -- cursor para fechamento das tags de grupo
        
          FOR i IN REVERSE 1 .. afechatag.count LOOP
          
            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN
            
              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                       WHEN g_tp_beneficio IN
                            (SELECT DISTINCT cod_esocial
                               FROM esocial.tsoc_par_sigeprev_esocial cse
                              WHERE cse.cod_tipo = 7
                                AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                        2
                       ELSE
                        1
                     END))
                 AND tev.nom_evento = 'evtCdBenAlt'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtCdBenAlt'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                               WHEN g_tp_beneficio IN
                                    (SELECT DISTINCT cod_esocial
                                       FROM esocial.tsoc_par_sigeprev_esocial cse
                                      WHERE cse.cod_tipo = 7
                                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                                2
                               ELSE
                                1
                             END))
                         AND tet.nom_registro_pai = afechatag(i));
            
            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_2416';
                gb_rec_erro.tipo_evento       := '2416';
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 2416';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde deverá ser fechado a tag e chamo a func passando parametro de fechamento
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_2416(afechatag(i), v_cod_ins, 'evtCdBenAlt', 'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);
          
          END LOOP;
        
          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado
          
            dbms_sql.column_value(n_cursor_control, x, v_valores);
            cxml := fc_set_valor_2416('evtCdBenAlt',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));
          
          END LOOP;
        
          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;
        
          --  dbms_output.put_line(cxml);
        
          v_qtd_registros := v_qtd_registros + 1;
        
          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_2416';
            gb_rec_erro.tipo_evento       := '2416';
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAÇÃO DO XML 2416';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            sp_gera_erro_processo;
          
            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;
          
        END;
      
      END LOOP;
    
      dbms_sql.close_cursor(n_cursor_control);
    
      v_faixa_ini := v_faixa_ini + 1;
    
    END LOOP;
  
    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);
  
  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_2410';
      gb_rec_erro.tipo_evento       := '2410';
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAÇÃO DO XML 2410';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);
    
  END sp_xml_2416;

  PROCEDURE sp_xml_2420(p_cod_ins IN NUMBER, p_id IN NUMBER) IS
  
    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
  
    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;
  
    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    vdata_ini     DATE;
    vdata_fim     DATE;
    vtp_beneficio VARCHAR2(4);
  
    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;
  
    v_valores VARCHAR2(100);
  
    FUNCTION fc_set_valor_2420(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS
    
      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;
    
      vxml   CLOB;
      vvalor VARCHAR2(100);
    
      --      raise_tam_invalido EXCEPTION;
    BEGIN
    
      vvalor := p_valor;
    
      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
               WHEN vtp_beneficio IN
                    (SELECT DISTINCT cod_esocial
                       FROM esocial.tsoc_par_sigeprev_esocial cse
                      WHERE cse.cod_tipo = 7
                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                2
               ELSE
                1
             END))
         AND tet.num_seq_coluna = p_num_seq_coluna;
    
      -- seto o valor no xml, dentro da tag passada como parametro
    
      IF vtip_elemento = 'A' THEN
      
        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);
      
      ELSE
      
        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);
      
      END IF;
      RETURN vxml;
    
    EXCEPTION
    
      WHEN no_data_found THEN
        RETURN p_xml;
      
      WHEN OTHERS THEN
      
        raise_application_error(-20001,
                                'Erro em fc_set_valor: ' || SQLERRM);
      
    END fc_set_valor_2420;
  
    FUNCTION fc_tag_2420(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS
    
      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    
    BEGIN
    
      -- identifico se o parametro é para abertura de tag
    
      IF p_abre_fecha = 'A' THEN
      
        -- verifico se há algum atributo a ser atribuido a tag de abertura e o seu respectivo valor
      
        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                 WHEN vtp_beneficio IN
                      (SELECT DISTINCT cod_esocial
                         FROM esocial.tsoc_par_sigeprev_esocial cse
                        WHERE cse.cod_tipo = 7
                          AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                  2
                 ELSE
                  1
               END))
           AND tet.nom_registro_pai = p_nom_registro;
      
        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';
      
      ELSIF p_abre_fecha = 'F' THEN
      
        vxml := '</' || p_nom_registro || '>';
      
      END IF;
    
      RETURN vxml;
    
    EXCEPTION
      WHEN no_data_found THEN
      
        -- caso não exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;
      
      WHEN OTHERS THEN
      
        raise_application_error(-20001, 'Erro em fc_tag: ' || SQLERRM);
      
    END fc_tag_2420;
  
  BEGIN
  
    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';
  
    -- Retorno query para montar cursor do detalhe do XML e informações fixas do header
  
    SELECT tsx.dsc_sql,
           tev.num_versao_xml,
           tev.dsc_encoding_xml,
           tev.num_cnpj_empregador
      INTO vdsc_sql,
           vnum_versao_xml,
           vdsc_encoding_xml,
           vnum_cnpj_empregador
      FROM tsoc_par_eventos_xml    tev,
           tsoc_par_estruturas_xml tet,
           tsoc_par_sql_xml        tsx
     WHERE tev.cod_ins = tet.cod_ins
       AND tev.cod_evento = tet.cod_evento
       AND tev.num_versao_evento = tet.num_versao_evento
       AND tev.cod_ins = tsx.cod_ins
       AND tev.cod_evento = tsx.cod_evento
       AND tev.num_versao_evento = tsx.num_versao_evento
       AND tsx.num_seq_sql = CASE
             WHEN vtp_beneficio IN
                  (SELECT DISTINCT cod_esocial
                     FROM esocial.tsoc_par_sigeprev_esocial cse
                    WHERE cse.cod_tipo = 7
                      AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
              2
             ELSE
              1
           END
       AND tev.cod_ins = p_cod_ins
       AND tev.nom_evento = 'evtCdBenTerm'
       AND tev.dat_fim_vig IS NULL
       AND tet.num_seq = 1
       AND tet.flg_sql = 'S'
       AND tet.flg_obrigatorio = 'S';
  
    vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);
    vdsc_sql    := vdsc_sql || ' WHERE id_pk = ' || p_id;
  
    OPEN cur_tag FOR vdsc_sql;
  
    -- atribuo um id referencia ao cursor e defino as colunas da query no cursor
  
    n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
    dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);
  
    FOR x IN 1 .. cur_count LOOP
    
      -- percorro o cursor e defino os valores para cada coluna
    
      dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);
    
    END LOOP;
  
    WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
    
      vdata_ini := SYSDATE;
      -- variavel para controlar array de fechamento das tags
      nfechatag := 1;
    
/*      cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
              vdsc_encoding_xml || '"?>' || chr(13);*/
    
      -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo
    
      FOR c_tag IN (SELECT tet.nom_registro,
                           tet.nom_registro_pai,
                           tet.tip_elemento,
                           tet.flg_sql,
                           tet.num_seq_sql,
                           tet.num_seq_coluna
                      FROM tsoc_par_eventos_xml    tev,
                           tsoc_par_estruturas_xml tet
                     WHERE tev.cod_ins = tet.cod_ins
                       AND tev.cod_evento = tet.cod_evento
                       AND tev.num_versao_evento = tet.num_versao_evento
                       AND tev.cod_ins = p_cod_ins
                       AND tev.nom_evento = 'evtCdBenTerm'
                       AND tet.tip_elemento IN ('G', 'CG', 'E')
                       AND tev.dat_fim_vig IS NULL
                       AND (tet.flg_obrigatorio = 'S' OR
                           (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                             WHEN vtp_beneficio IN
                                  (SELECT DISTINCT cod_esocial
                                     FROM esocial.tsoc_par_sigeprev_esocial cse
                                    WHERE cse.cod_tipo = 7
                                      AND upper(des_esocial) LIKE
                                          '%PENS_O%MORTE%') THEN
                              2
                             ELSE
                              1
                           END))
                     ORDER BY num_seq ASC) LOOP
      
        -- identifico se é uma tag de grupo (tags que não possuem valores associados, apenas atributos)
        IF c_tag.tip_elemento IN ('G', 'CG') THEN
        
          -- adiciono no array auxiliar para fechamento das tags
        
          afechatag(nfechatag) := c_tag.nom_registro;
        
          -- chamo a func de montar tag, passando parametro de abertura de tag
        
          cxml := cxml || fc_tag_2420(c_tag.nom_registro,
                                      p_cod_ins,
                                      'evtCdBenTerm',
                                      'A') || chr(13);
        
          nfechatag := nfechatag + 1;
        ELSE
          -- caso seja uma tag de elemento (tags que possuem valor associado)
        
          cxml := cxml || fc_tag_2420(c_tag.nom_registro,
                                      p_cod_ins,
                                      'evtCdBenTerm',
                                      'A');
        
          -- chamo func de montar tag com parametro de fechamento de tag
          cxml := cxml || fc_tag_2420(c_tag.nom_registro,
                                      p_cod_ins,
                                      'evtCdBenTerm',
                                      'F') || chr(13);
        
        END IF;
      
      END LOOP;
    
      -- cursor para fechamento das tags de grupo
    
      FOR i IN REVERSE 1 .. afechatag.count LOOP
      
        -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
        -- onde devemos fechar a tag
      
        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tet.tip_elemento IN ('G', 'CG', 'E')
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                 WHEN vtp_beneficio IN
                      (SELECT DISTINCT cod_esocial
                         FROM esocial.tsoc_par_sigeprev_esocial cse
                        WHERE cse.cod_tipo = 7
                          AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                  2
                 ELSE
                  1
               END))
           AND tev.nom_evento = 'evtCdBenTerm'
           AND tet.nom_registro_pai = afechatag(i)
           AND num_seq =
               (SELECT MAX(num_seq)
                  FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
                 WHERE tev.cod_ins = tet.cod_ins
                   AND tev.cod_evento = tet.cod_evento
                   AND tev.num_versao_evento = tet.num_versao_evento
                   AND tev.cod_ins = p_cod_ins
                   AND tev.nom_evento = 'evtCdBenTerm'
                   AND tet.tip_elemento IN ('G', 'CG', 'E')
                   AND tev.dat_fim_vig IS NULL
                   AND (tet.flg_obrigatorio = 'S' OR
                       (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                         WHEN vtp_beneficio IN
                              (SELECT DISTINCT cod_esocial
                                 FROM esocial.tsoc_par_sigeprev_esocial cse
                                WHERE cse.cod_tipo = 7
                                  AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                          2
                         ELSE
                          1
                       END))
                   AND tet.nom_registro_pai = afechatag(i));
      
        -- identifico o ponto onde deverá ser fechado a tag e chamo a func passando parametro de fechamento
        cxml := substr(cxml,
                       1,
                       (instr(cxml, vnom_registro, -1)) +
                       length(vnom_registro) + 1) ||
                fc_tag_2420(afechatag(i), p_cod_ins, 'evtCdBenTerm', 'F') ||
                substr(cxml,
                       (instr(cxml, vnom_registro, -1)) +
                       length(vnom_registro) + 1);
      
      END LOOP;
    
      FOR x IN 1 .. cur_count LOOP
      
        -- seta no xml os valores retornados pelo cursor parametrizado
      
        dbms_sql.column_value(n_cursor_control, x, v_valores);
        cxml := fc_set_valor_2420('evtCdBenTerm',
                                  p_cod_ins,
                                  cxml,
                                  v_valores,
                                  to_number(cur_desc(x).col_name));
      
      END LOOP;
    
      vdata_fim := SYSDATE;
    
      /*      UPDATE tsoc_1010_rubrica r
        SET r.xml_envio        = cxml,
            r.ctr_dat_ini_proc = vdata_ini,
            r.ctr_dat_fim_proc = vdata_fim
      WHERE r.rowid = v_valores;*/
    
      /*      INSERT INTO tsoc_analise_tempo_geracao atg
        (cod_rubrica, xml_proc, dat_ini_proc, dat_fim_proc)
      VALUES
        (vcod_rubr, cxml, vdata_ini, vdata_fim);
      
      COMMIT;*/
    
      EXECUTE IMMEDIATE 'UPDATE ' || vtab_update || ' SET XML_ENVIO = ''' || cxml ||
                        ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' || p_id;
    
      COMMIT;
      --  dbms_output.put_line(cxml);
    
    END LOOP;
  
    dbms_sql.close_cursor(n_cursor_control);
  
    /* EXCEPTION
    WHEN OTHERS THEN
    
      raise_application_error(-20001,
                              'Erro em sp_xml_1p1: ' || SQLERRM);*/
  
  END sp_xml_2420;

  PROCEDURE sp_arqs_qualificacao_cadastral AS
  
    vfile1 VARCHAR2(1000); --Aposentadoria Civil
    vtype1 utl_file.file_type;
  
    vfile2 VARCHAR2(1000); --Aposentadoria Militar
    vtype2 utl_file.file_type;
  
    vfile3 VARCHAR2(1000); --Pensão Civil
    vtype3 utl_file.file_type;
  
    vfile4 VARCHAR2(1000); --Pensão Militar
    vtype4 utl_file.file_type;
  
    vdir VARCHAR2(25) := 'ARQS_REL_GERAIS';
  
    CURSOR c_aposen_civil IS
      WITH got_tenth AS
       (SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               ntile(10) over(ORDER BY pf.cod_ide_cli) AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio <> 'M'
           AND cb.cod_entidade <> 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
        UNION
        SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               10 AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio <> 'M'
           AND cb.cod_entidade <> 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
           AND length(TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                         '[^[:alpha:][ ]]*',
                                                         ''),
                                          '[[:space:]]+',
                                          chr(32)))) > 60)
      SELECT cpf || ';' || nis || ';' || nome || ';' || "DN" AS vs_dados
        FROM got_tenth
       WHERE rownum < 1001
       ORDER BY nome;
  
    CURSOR c_aposen_militar IS
      WITH got_tenth AS
       (SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               ntile(10) over(ORDER BY pf.cod_ide_cli) AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio <> 'M'
           AND cb.cod_entidade = 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
        UNION
        SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               10 AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio <> 'M'
           AND cb.cod_entidade = 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
           AND length(TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                         '[^[:alpha:][ ]]*',
                                                         ''),
                                          '[[:space:]]+',
                                          chr(32)))) > 60)
      SELECT cpf || ';' || nis || ';' || nome || ';' || "DN" AS vs_dados
        FROM got_tenth
       WHERE rownum < 1001
       ORDER BY nome;
  
    CURSOR c_pensao_civil IS
      WITH got_tenth AS
       (SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               ntile(10) over(ORDER BY pf.cod_ide_cli) AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio = 'M'
           AND cb.cod_entidade <> 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
        UNION
        SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               10 AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio = 'M'
           AND cb.cod_entidade <> 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
           AND length(TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                         '[^[:alpha:][ ]]*',
                                                         ''),
                                          '[[:space:]]+',
                                          chr(32)))) > 60)
      SELECT cpf || ';' || nis || ';' || nome || ';' || "DN" AS vs_dados
        FROM got_tenth
       WHERE rownum < 1001
       ORDER BY nome;
  
    CURSOR c_pensao_militar IS
      WITH got_tenth AS
       (SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               ntile(10) over(ORDER BY pf.cod_ide_cli) AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio = 'M'
           AND cb.cod_entidade = 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
        UNION
        SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               10 AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio = 'M'
           AND cb.cod_entidade = 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
           AND length(TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                         '[^[:alpha:][ ]]*',
                                                         ''),
                                          '[[:space:]]+',
                                          chr(32)))) > 60)
      SELECT cpf || ';' || nis || ';' || nome || ';' || "DN" AS vs_dados
        FROM got_tenth
       WHERE rownum < 1001
       ORDER BY nome;
  
  BEGIN
  
    --1. Aposentadoria Civil
    vfile1 := 'D.CNS.CPF.001.' || to_char(SYSDATE, 'RRRRMMDDHH24MISS') ||
              '.APOSENTADORIA_CIVIL.TXT';
  
    vtype1 := utl_file.fopen(vdir, vfile1, 'W', 32767);
  
    FOR reg IN c_aposen_civil LOOP
    
      --Gravando a linha
      utl_file.put_line(vtype1, reg.vs_dados);
    
    END LOOP;
  
    utl_file.fclose(vtype1);
  
    --2. Aposentadoria Militar
    vfile2 := 'D.CNS.CPF.001.' || to_char(SYSDATE, 'RRRRMMDDHH24MISS') ||
              '.APOSENTADORIA_MILITAR.TXT';
  
    vtype2 := utl_file.fopen(vdir, vfile2, 'W', 32767);
  
    FOR reg IN c_aposen_militar LOOP
    
      --Gravando a linha
      utl_file.put_line(vtype2, reg.vs_dados);
    
    END LOOP;
  
    utl_file.fclose(vtype2);
  
    --3. Pensão Civil
    vfile3 := 'D.CNS.CPF.001.' || to_char(SYSDATE, 'RRRRMMDDHH24MISS') ||
              '.PENSAO_CIVIL.TXT';
  
    vtype3 := utl_file.fopen(vdir, vfile3, 'W', 32767);
  
    FOR reg IN c_pensao_civil LOOP
    
      --Gravando a linha
      utl_file.put_line(vtype3, reg.vs_dados);
    
    END LOOP;
  
    utl_file.fclose(vtype3);
  
    --4. Pensão Militar
    vfile4 := 'D.CNS.CPF.001.' || to_char(SYSDATE, 'RRRRMMDDHH24MISS') ||
              '.PENSAO_MILITAR.TXT';
  
    vtype4 := utl_file.fopen(vdir, vfile4, 'W', 32767);
  
    FOR reg IN c_pensao_militar LOOP
    
      --Gravando a linha
      utl_file.put_line(vtype4, reg.vs_dados);
    
    END LOOP;
  
    utl_file.fclose(vtype4);
  
  EXCEPTION
    WHEN OTHERS THEN
      utl_file.fclose(vtype1);
      utl_file.fclose(vtype2);
      utl_file.fclose(vtype3);
      utl_file.fclose(vtype4);
  END sp_arqs_qualificacao_cadastral;

END pac_esocial_xml;
/
