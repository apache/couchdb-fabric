%% @author gilv
%% @doc @todo Add description to fabric_swift_handler.


-module(fabric_attachments_handler).

-include_lib("fabric/include/fabric.hrl").
-include_lib("couch/include/couch_db.hrl").

-export([inline_att_store/2, inline_att_handler/3, container_handler/2, normal_att_store/5, externalize_att/1]).


%% ====================================================================
%% External API functions. I try to keep them as general as possible, 
%% without correlation to specific object store that might be internally used.
%% ====================================================================

normal_att_store(FileName,Db,ContentLen,MimeType,Req) ->
    ContainerName = container_name(Db),
    couch_log:debug("Standard attachment handler",[]),
    couch_log:debug("Going to store ~p of length is ~p,  ~p in the container: ~n",[FileName,ContentLen,ContainerName]),
    %Bad implementation - no chunk reader. All kept in the memory. Should be fixed.
    Data = couch_httpd:recv(Req, ContentLen),
    case swift_put_object(ContainerName, FileName, MimeType, [], Data) of
        {ok,{201,NewUrl}} ->
            couch_log:debug("Created. Swift response code is 201 ~n",[]),
            {ObjectSize,EtagMD5} = swift_head_object(ContainerName, FileName),
            {unicode:characters_to_binary(NewUrl, unicode, utf8), ObjectSize, EtagMD5};
        {_,{Code,_}} ->
            couch_log:debug("Swift response code is ~p~n",[]),
            {Data, -1}
    end.

inline_att_store(Db, Docs) ->
    DbName = container_name(Db),
    couch_log:debug("Store inline base64 encoded attachment ~n",[]),
    if 
        is_list(Docs)  ->
            couch_log:debug("Going to handle document list",[]),
            DocsArray = Docs;
        true ->
            couch_log:debug("Going to handle single document",[]),
            DocsArray = [Docs]
    end,
    [#doc{atts=Atts0} = Doc | Rest] = DocsArray,
    if 
        length(Atts0) > 0 ->
            couch_log:debug("Att length is larger than 0",[]),
            Atts = [att_processor(DbName,Att) || Att <- Atts0],
            if 
                is_list(Docs)  ->
                    [Doc#doc{atts = Atts} | Rest];
            true ->
                Doc#doc{atts = Atts}
            end;
        true ->
            couch_log:debug("No attachments to handle",[]),
            Docs
    end.

inline_att_handler(get, Db, Att) ->
    DbName = container_name(Db),
    couch_log:debug("Retrieve attachment",[]),
    [Data,Name,AttLen,AttLenUser] = couch_att:fetch([data, name,att_len, att_external_size],Att),
    couch_log:debug("Going to retrieve ~p. DB length is: ~p, stored length: ~p~n",[Name, AttLen, AttLenUser]),
    NewData = swift_get_object(DbName, Name),
    NewAtt = couch_att:store([{data, NewData},{att_len,AttLenUser},{disk_len,AttLenUser}], Att),
    NewAtt;

inline_att_handler(delete,Db, Att) ->
    DbName = container_name(Db),
    couch_log:debug("Delete attachment ~p~n",[]).

container_handler(create,DbName) ->
    couch_log:debug("Create container ~p~n",[DbName]),
    case swift_create_container(DbName) of
        {ok,{201,Container}} ->
            couch_log:debug("Container ~p created succesfully ~n",[Container]),
            {ok,Container};
        {error,_} ->
            couch_log:debug("Container ~p creation failed ~n",[DbName]),
            {error,DbName}
    end;
container_handler(get,DbName) ->
    couch_log:debug("Get container ~p~n",[DbName]);
container_handler(delete,DbName)->
    couch_log:debug("Delete container ~p~n",[DbName]),
    swift_delete_container(DbName).

externalize_att(Db) ->
    Res = config:get("swift","attachments_offload","false"),
    Res.

%% ====================================================================
%% Internal general functions
%% ====================================================================

container_name(Db) ->
    Suffix = list_to_binary(mem3:shard_suffix(Db)),
    DbNameSuffix = erlang:iolist_to_binary([fabric:dbname(Db), Suffix]),
    couch_log:debug("Db to Container name ~p~n",[DbNameSuffix]),	
    DbNameSuffix.

hex_to_bin(Str) -> << << (erlang:list_to_integer([H], 16)):4 >> || H <- Str >>.

%% ====================================================================
%% Internal OpenStack Swift implementation
%% ====================================================================

extractAuthInfo([{_,_}|_] = Obj,[])-> 
    Storage_URL = proplists:get_value(<<"publicURL">>, Obj),
    Storage_Token = proplists:get_value(<<"id">>, Obj),
    Status = true,
    [Storage_URL, Storage_Token, Status];
extractAuthInfo([{[{_,_}|_]}] = Obj,[])->
    [{ObjNext}] = Obj,
    Storage_URL = proplists:get_value(<<"publicURL">>, ObjNext),
    Storage_Token = proplists:get_value(<<"id">>, ObjNext),
    Status = true,
    [Storage_URL, Storage_Token, Status];
extractAuthInfo([{_,_}|L]=Obj,KeyValPath)->
    [{Key, Val}|KeyValPathNext] = KeyValPath,
    ObjVal = proplists:get_value(Key,Obj),
    case Val of
        [] -> extractAuthInfo(ObjVal, KeyValPathNext);
            ObjVal -> extractAuthInfo(Obj, KeyValPathNext);
        _ -> ["", "", false]
    end;
extractAuthInfo([{[{_,_}|_]}|L]=Obj,KeyValPath)->
    [ObjCur| _] = Obj,
    [Storage_URL, Storage_Token, Status] = extractAuthInfo(ObjCur, KeyValPath),
    case Status of
         false -> extractAuthInfo(L, KeyValPath);
         _ -> [Storage_URL, Storage_Token, Status]
    end;
extractAuthInfo(Obj,KeyValPath) when is_tuple(Obj)->
    {Doc} = Obj,
    extractAuthInfo(Doc, KeyValPath).

att_processor(DbName,Att) ->
    couch_log:debug("Swift: attachment processor",[]),
    [Type, Enc, DiskLen, AttLen] = couch_att:fetch([type, encoding, disk_len, att_len], Att),
    [Name, Data] = couch_att:fetch([name, data], Att),
    couch_log:debug("Swift: att name: ~p, type: ~p, encoding: ~p, disk len: ~p~n",[Name,Type,Enc,DiskLen]),
    case is_binary(Data) of
        true ->
            couch_log:debug("Swift: binary attachment exists",[]),
            case swift_put_object(DbName, Name, Type, [], Data) of
                {ok,{201,NewUrl}} ->
                    N1 = unicode:characters_to_binary(NewUrl, unicode, utf8),
                    couch_log:debug("~p ~p~n",[N1, NewUrl]),
                    NewAtt = couch_att:store(data,N1,Att),
                    couch_log:debug("Swift. testing store in original length ~p~n",[AttLen]),
                    {ObjectSize,EtagMD5} = swift_head_object(DbName, Name),
                    NewAtt1 = couch_att:store([{att_external_size,ObjectSize},{att_external,"external"},{att_external_md5,EtagMD5}],NewAtt),
                    NewAtt1;
                {_,{Code,_}} ->
                    couch_log:debug("Swift: response code is ~p~n",[Code]),
                    Att
             end;
        _ -> 
            Att
    end.

swift_put_object(Container, ObjName, ContextType, CustomHeaders, Data) ->
    couch_log:debug("Swift: PUT ~p/~s object method ~n",[Container, ObjName]),
    case authenticate() of
        {ok,{Url,Storage_Token}} ->
            ObjNameEncoded = couch_util:url_encode(ObjName),
            Headers = CustomHeaders ++ [{"X-Auth-Token",Storage_Token}],
            Method = put,
            couch_log:debug("Swift: going to upload : ~p ~p ~p ~p ~p~n",[Url, Headers, ContextType,ObjNameEncoded, Container]),
            NewUrl = Url ++ "/" ++ unicode:characters_to_list(Container) ++ "/" ++ unicode:characters_to_list(ObjNameEncoded),
            couch_log:debug("Swift: url: ~p~n",[NewUrl]),
            R = httpc:request(Method, {NewUrl, Headers, unicode:characters_to_list(ContextType), Data}, [], []),
            couch_log:debug("~p~n",[R]),
            case R of
                {ok,_} ->
                    {ok, {{"HTTP/1.1",ReturnCode, _}, _, _}} = R,
                    {ok,{ReturnCode,NewUrl}};	
                Error ->
                    {error,{element(2,Error),NewUrl}}	
            end;
        {not_authenticated,_} ->
            {error,"Not authenticated",""};
        {auth_not_supported, Error} ->
            {error,{element(2,Error),""}}
    end.

swift_delete_object(Container, ObjName) ->
    couch_log:debug("Swift: Delete ~p/~p object method ~n",[Container, ObjName]).

swift_get_object(Container, ObjName) ->
    couch_log:debug("Swift: get object ~p/~p ~n",[Container, ObjName]),
    case authenticate() of
        {ok,{Url,Storage_Token}} ->
            Header = [{"X-Auth-Token",Storage_Token}],
            ObjNameEncoded = couch_util:url_encode(ObjName),
            Method = get,
            NewUrl = Url  ++ "/" ++ unicode:characters_to_list(Container) ++ "/" ++ unicode:characters_to_list(ObjNameEncoded),
            couch_log:debug("Swift: url ~p~n",[NewUrl]),
            R = httpc:request(Method, {NewUrl, Header}, [], []),
            {ok, {{"HTTP/1.1",ReturnCode, _}, Head, Body}} = R,
            couch_log:debug("Swift: ~p~p ~n",[ReturnCode, Head]),			
            Body;
        {not_authenticated,_} ->
            {error, "Not authenticated"};
        {not_supported, Error} ->
            {error,{element(2,Error),""}}
    end.

swift_head_object(Container, ObjName) ->
    couch_log:debug("Swift: head object ~p/~p ~n",[Container, ObjName]),
    case authenticate() of
        {ok,{Url,Storage_Token}} ->
            Header = [{"X-Auth-Token",Storage_Token}],
            Method = head,
            ObjNameEncoded = couch_util:url_encode(ObjName),
            NewUrl = Url  ++ "/" ++ unicode:characters_to_list(Container) ++ "/" ++ unicode:characters_to_list(ObjNameEncoded),
            couch_log:debug("Swift: url ~p~n",[NewUrl]),
            R = httpc:request(Method, {NewUrl, Header}, [], []),
            {ok, {{"HTTP/1.1",ReturnCode, _}, Head, _}} = R,
            couch_log:debug("Swift: ~p~p~n",[ReturnCode, Head]),	
            ObjectSize = lists:filter(fun ({"content-length",_}) -> true ; (_) -> false end, Head),
            EtagHeader = lists:filter(fun ({"etag",_}) -> true ; (_) -> false end, Head),
            Etag = element(2,lists:nth(1,EtagHeader)),
            {ObjectSizeNumeric,_} = string:to_integer(element(2,lists:nth(1,ObjectSize))),
            couch_log:debug("Swift: Object size is: ~p and Etag: ~p~n",[ObjectSizeNumeric, Etag]),
            couch_log:debug("Etag in base16 ~p~n",[Etag]),
            EtagDecode = hex_to_bin(Etag),
            EtagMD5 = base64:encode(EtagDecode),
            couch_log:debug("Etag in base64 ~p~n",[EtagMD5]),
            {ObjectSizeNumeric,EtagMD5};
        {auth_not_supported, Error} ->
            {error,{element(2,Error),""}};
        {not_authenticated, _} ->
            {-1,""}
    end.

swift_create_container(DbName) ->
    couch_log:debug("Swift : create container ~p~n",[DbName]),
    case authenticate() of
        {ok,{Url,Storage_Token}} ->
            Header = [{"X-Auth-Token",Storage_Token}],
            Method = put,
            Container = DbName,
            NewUrl = Url ++ "/" ++ unicode:characters_to_list(Container) ++"/",
            couch_log:debug("Swift: url ~p ~n",[NewUrl]),
            R = httpc:request(Method, {NewUrl, Header, [], []}, [], []),
            case R of
                {ok,_} ->
                    {ok, {{"HTTP/1.1",ReturnCode, _}, _, _}} = R,
                    couch_log:debug("Swift: container ~p created with code : ~p~n",[Container, ReturnCode]),
                    {ok,{ReturnCode,Container}};	
                Error ->
                    couch_log:debug("Swift: container ~p creation failed : ~p~n",[Container, element(2,Error)]),
                    {error,{element(2,Error),Container}}	
            end;
        {not_authenticated,_} ->
            {error, "Not authenticated"};
        {not_supported, Error} ->
            {error,{element(2,Error),""}}
    end.

swift_delete_container(Container) ->
    case authenticate() of
        {ok,{Url,Storage_Token}} ->
        Header = [{"X-Auth-Token",Storage_Token}],
        Method = delete,
        NewUrl = Url ++ "/" ++ Container ++"/",
        R = httpc:request(Method, {NewUrl, Header, [], []}, [], []),
        case R of
            {ok,_} ->
                {ok, {{"HTTP/1.1",ReturnCode, _}, _, _}} = R,
                    {ok,{ReturnCode,Container}};	
                Error ->
                    {error,{element(2,Error),Container}}	
        end;
        {not_authenticated, _} ->
            {error, "Not authenticated"};
        {not_supported, Error} ->
            {error,{element(2,Error),""}}
    end.

authenticate() ->
    couch_log:debug("Going to authenticate in Swift",[]),
    case config:get("swift", "auth_model", "tempauth") of
        "tempauth" ->
            Method = get,
            URL = config:get("swift", "auth_url"),
            Header = [{"X-Storage-User", config:get("swift", "account_tenant") ++ ":" ++ config:get("swift", "username")},{"X-Storage-Pass", config:get("swift", "password")}],
            couch_log:debug("Going to send authentication request:  ~p~n",[URL]),
            R = httpc:request(Method, {URL, Header}, [], []),
            {ok, {{"HTTP/1.1",ReturnCode, _}, Head, _}} = R,
            case ReturnCode of
                200 ->
                    couch_log:debug("swift: authenticated: ~p~n",[ReturnCode]),
                    Storage_URL_F = fun ({"x-storage-url",_}) -> true ; (_) -> false end,
                    Storage_URL = lists:filter(Storage_URL_F, Head),
                    Storage_Token_F = fun ({"x-storage-token",_}) -> true ; (_) -> false end,
                    Storage_Token = lists:filter(Storage_Token_F, Head),
                    {ok,{element(2,lists:nth(1,Storage_URL)), element(2,lists:nth(1,Storage_Token))}};
                _ ->
                    couch_log:debug("swift: not authenticated: ~p~n",[ReturnCode]),
                    {not_authenticated, ""}
            end;
        "keystone" ->
            Method = post,
            URL = config:get("swift", "auth_url"),
            Header = [],
            Type = "application/json",
            Tenant = config:get("swift", "account_tenant"),
            User = config:get("swift", "username"),
            Psw = config:get("swift", "password"),
            Body = "{\"auth\": {\"tenantName\": \"" ++ Tenant ++ "\", \"passwordCredentials\": {\"username\": \"" ++ User ++ "\", \"password\": \"" ++ Psw ++ "\"}}}",
            HTTPOptions = [],
            Options = [],
            R = httpc:request(Method, {URL, Header, Type, Body}, HTTPOptions, Options),
            {ok, {{"HTTP/1.1",ReturnCode, State}, _, Response}} = R,
            case ReturnCode of
                200 ->
                    couch_log:debug("swift: authenticated: ~p~n",[ReturnCode]),
                    Json = jiffy:decode(Response),
                                AuthInfoBinary = extractAuthInfo(Json, [{<<"access">>, ""}, {<<"serviceCatalog">>, ""}, {<<"name">>, <<"swift">>}, {<<"endpoints">>, ""}]),
                                {ok, {binary_to_list(lists:nth(1,AuthInfoBinary)), binary_to_list(lists:nth(2,AuthInfoBinary))}};
                _ ->
                    couch_log:debug("swift: not authenticated: ~p~n",[ReturnCode]),
                                {not_authenticated, ""}
            end;
        Error ->
            {auth_not_supported, Error}
    end.

