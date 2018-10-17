CREATE OR REPLACE FUNCTION assina_xml (p_xml  IN  CLOB)
  RETURN CLOB
AS
  l_request   soap_api.t_request;
  l_response  soap_api.t_response;

  l_return       CLOB;
  l_param_encode VARCHAR2(32767);
  l_url          VARCHAR2(32767);
  l_namespace    VARCHAR2(32767);
  l_method       VARCHAR2(32767);
  l_soap_action  VARCHAR2(32767);
  l_result_name  VARCHAR2(32767);
BEGIN
  l_param_encode:= DBMS_XMLGEN.CONVERT(p_xml);
  l_url         := 'http://10.32.36.15:7201/assianturaws/services/Assinatura';
  l_namespace   := 'xmlns="http://DefaultNamespace"';
  l_method      := 'getAssinatura';
  l_soap_action := 'http://10.32.36.15:7201/assianturaws/services/Assinatura?wsdl';
  l_result_name := 'getAssinaturaReturn';

  l_request := soap_api.new_request(p_method       => l_method,
                                    p_namespace    => l_namespace);

  soap_api.add_parameter(p_request => l_request,
                         p_name    => 'xml',
                         p_type    => 'xsd:string',
                         p_value   => l_param_encode);


  l_response := soap_api.invoke(p_request => l_request,
                                p_url     => l_url,
                                p_action  => l_soap_action);

  l_return := soap_api.get_return_value(p_response  => l_response,
                                        p_name      => l_result_name,
                                        p_namespace => l_namespace);

  dbms_output.put_line(DBMS_XMLGEN.CONVERT(l_return,1));

  RETURN DBMS_XMLGEN.CONVERT(l_return,1);
END;
/
