% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(fabric_swift_driver).

-export([put_object/5, delete_object/2, get_object/2, head_object/2]).
-export([store_id/0, create_container/1, delete_container/1 ]).
%% ====================================================================
%% OpenStack Swift implementation
%% ====================================================================

store_id() ->
    "swift".

put_object(Container, ObjName, ContextType, CustomHeaders, Data) ->
    couch_log:debug("Swift: PUT ~p/~s object ~n", [Container, ObjName]),
    case authenticate() of
        {ok, {Url, Storage_Token}} ->
            ObjNameEncoded = couch_util:url_encode(ObjName),
            Headers = CustomHeaders ++ [{"X-Auth-Token", Storage_Token},
                                        {"Content-Type",
                                         unicode:characters_to_list(ContextType)}],
            Method = put,
            couch_log:debug("Swift: PUT : ~p ~p ~p ~p~n",
                            [Url,ContextType, ObjNameEncoded, Container]),
            NewUrl = Url ++ "/" ++ unicode:characters_to_list(Container) ++ "/" ++ unicode:characters_to_list(ObjNameEncoded),
            couch_log:debug("Swift: PUT url: ~p~n",[NewUrl]),
            R = ibrowse:send_req(NewUrl, Headers, Method, Data),
            case R of
                {ok, ReturnCode, _, _} ->
                    {ok, {ReturnCode, NewUrl}};	
                Error ->
                    couch_log:debug("PUT object failed  ~p ~n", [element(2, Error)]),
                    {error, "PUT object failed"}	
            end;
        {not_authenticated, _} ->
            {error, "Not authenticated"}
    end.

delete_object(Container, ObjName) ->
    %TO-DO: should be called during database compaction. 
    couch_log:debug("Swift: Delete ~p/~p ~n", [Container, ObjName]),
    case authenticate() of
        {ok, {Url, Storage_Token}} ->
            Header = [{"X-Auth-Token", Storage_Token}],
            ObjNameEncoded = couch_util:url_encode(ObjName),
            Method = delete,
            NewUrl = Url  ++ "/" ++ unicode:characters_to_list(Container) ++ "/"
                ++ unicode:characters_to_list(ObjNameEncoded),
            couch_log:debug("Swift: url ~p~n", [NewUrl]),
            R = ibrowse:send_req(NewUrl, Header, Method),
            case R of
                {ok, ReturnCode, _, _} ->
                    couch_log:debug("Swift: Delete ~p. Return code ~p ~n",
                                    [NewUrl, ReturnCode]),           
                    {ok, ReturnCode};
                Error ->
                    couch_log:debug("Swift: Delete ~p failed. ~n", [NewUrl]),
                    couch_log:debug("Swift: ~p ~n", [element(2, Error)]),
                    {error, "Delete object failed"}
            end;
        {not_authenticated, _} ->
            {error, "Not authenticated"}
    end.

get_object(Container, ObjName) ->
    couch_log:debug("Swift: get object ~p/~p ~n", [Container, ObjName]),
    case authenticate() of
        {ok, {Url, Storage_Token}} ->
            Header = [{"X-Auth-Token", Storage_Token}],
            ObjNameEncoded = couch_util:url_encode(ObjName),
            Method = get,
            NewUrl = Url  ++ "/" ++ unicode:characters_to_list(Container) ++ "/"
                ++ unicode:characters_to_list(ObjNameEncoded),
            couch_log:debug("Swift: url ~p~n", [NewUrl]),
            R = ibrowse:send_req(NewUrl, Header, Method),
            case R of
                {ok, ReturnCode, _, Body} ->
                    couch_log:debug("Swift: GET ~p with return code ~p ~n", [NewUrl, ReturnCode]),			
                    Body;
                Error ->
                    couch_log:debug("Swift: GET ~p failed. ~n", [NewUrl]),
                    couch_log:debug("Swift: ~p ~n", [element(2, Error)]),
                    {error, "Get object failed"}
            end;
        {not_authenticated, _} ->
            {error, "Not authenticated"}
    end.

head_object(Container, ObjName) ->
    couch_log:debug("Swift: head object ~p/~p ~n", [Container, ObjName]),
    case authenticate() of
        {ok, {Url, Storage_Token}} ->
            Header = [{"X-Auth-Token", Storage_Token}],
            Method = head,
            ObjNameEncoded = couch_util:url_encode(ObjName),
            NewUrl = Url  ++ "/" ++ unicode:characters_to_list(Container)
                ++ "/" ++ unicode:characters_to_list(ObjNameEncoded),
            couch_log:debug("Swift: url ~p~n", [NewUrl]),
            R = ibrowse:send_req(NewUrl, Header, Method),
            case R of
                {ok, ReturnCode, Head, _} ->
                    couch_log:debug("Swift: ~p~p~n", [ReturnCode, Head]),	
                    ObjectSize = lists:filter(fun ({"Content-Length", _}) -> true; (_) -> false end, Head),
                    EtagHeader = lists:filter(fun ({"Etag", _}) -> true ; (_) -> false end, Head),
                    Etag = element(2, lists:nth(1, EtagHeader)),
                    {ObjectSizeNumeric, _} = string:to_integer(element(2, lists:nth(1, ObjectSize))),
                    couch_log:debug("Swift: Object size is: ~p and Etag: ~p~n",
                                    [ObjectSizeNumeric, Etag]),
                    EtagDecode = hex_to_bin(Etag),
                    EtagMD5 = base64:encode(EtagDecode),
                    couch_log:debug("Etag in base64 ~p~n", [EtagMD5]),
                    {ok, {ObjectSizeNumeric, EtagMD5}};
                Error ->
                    couch_log:debug("Swift: Head ~p failed ~n", [NewUrl]),
                    couch_log:debug("Swift: ~p ~n", [element(2, Error)]),
                    {error,"HEAD object failed"}
            end;
        {not_authenticated, _} ->
            {error, "Not authenticated"}
    end.

create_container(DbName) ->
    couch_log:debug("Swift : create container ~p~n", [DbName]),
    case authenticate() of
        {ok, {Url, Storage_Token}} ->
            Header = [{"X-Auth-Token",Storage_Token}],
            Method = put,
            Container = DbName,
            NewUrl = Url ++ "/" ++ unicode:characters_to_list(Container) ++"/",
            couch_log:debug("Swift: url ~p ~n",[NewUrl]),
            R = ibrowse:send_req(NewUrl, Header, Method),
            case R of
                {ok, ReturnCode, _, _} ->
                    couch_log:debug("Swift: container ~p created with code : ~p~n",[Container, ReturnCode]),
                    {ok,{ReturnCode,Container}};	
                Error ->
                    couch_log:debug("Swift: container ~p creation failed : ~p~n",[Container, element(2,Error)]),
                    {error, "Failed to create container"}	
            end;
        {not_authenticated,_} ->
            {error, "Failed to create container. Not authenticated"}
    end.

delete_container(Container) ->
    case authenticate() of
        {ok,{Url,Storage_Token}} ->
        Header = [{"X-Auth-Token",Storage_Token}],
        Method = delete,
        NewUrl = Url ++ "/" ++ Container ++"/",
        R = ibrowse:send_req(NewUrl, Header, Method),
        case R of
            {ok, ReturnCode, _, _} ->
                    {ok,{ReturnCode,Container}};	
                Error ->
                    {error,{element(2,Error),Container}}	
        end;
        {not_authenticated, _} ->
            {error, "Not authenticated"};
        {not_supported, Error} ->
            {error,{element(2,Error),""}}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

hex_to_bin(Str) -> << << (erlang:list_to_integer([H], 16)):4 >> || H <- Str >>.

authenticate() ->
    couch_log:debug("Going to authenticate in Swift",[]),
    case config:get("swift", "auth_model", "tempauth") of
        "tempauth" ->
            swift_v1_auth();
        "keystone" ->
            keystone_auth();
        Error ->
            couch_log:debug("Authentication method is not supported: ~p", element(2,Error)),
            {not_authenticated, ""}
    end.

keystone_auth() ->
    Method = post,
    URL = config:get("swift", "auth_url"),
    Header = [{"Content-Type", "application/json"}],
    Tenant = config:get("swift", "account_tenant"),
    User = config:get("swift", "username"),
    Psw = config:get("swift", "password"),
    Body = "{\"auth\": {\"tenantName\": \"" ++ Tenant
            ++ "\", \"passwordCredentials\": {\"username\": \""
            ++ User ++ "\", \"password\": \"" ++ Psw ++ "\"}}}",
    R = ibrowse:send_req(URL, Header, Method, Body),
    {ok, ReturnCode, _, Response} = R,
    case ReturnCode of
        "200" ->
            couch_log:debug("keystone: authenticated: ~p~n", [ReturnCode]),
            Json = jiffy:decode(Response), 
                AuthInfoBinary = extractAuthInfo(Json, [{<<"access">>, ""},
                                                        {<<"serviceCatalog">>, ""},
                                                        {<<"name">>, <<"swift">>},
                                                        {<<"endpoints">>, ""}]),
                {ok, {binary_to_list(lists:nth(1, AuthInfoBinary)),
                      binary_to_list(lists:nth(2, AuthInfoBinary))}};
        _ ->
            couch_log:debug("keystone: not authenticated: ~p~n", [ReturnCode]),
            {not_authenticated, ""}
    end.

swift_v1_auth() ->
    Method = get,
    URL = config:get("swift", "auth_url"),
    Header = [{"X-Storage-User", config:get("swift", "account_tenant") ++ ":"
              ++ config:get("swift", "username")},
              {"X-Storage-Pass", config:get("swift", "password")}],
    R = ibrowse:send_req(URL, Header, Method),
    {ok, ReturnCode, Head, _} = R,
    case ReturnCode of
        "200" ->
            couch_log:debug("swift v1: authenticated: ~p~n", [ReturnCode]),
            Storage_URL_F = fun ({"X-Storage-Url", _}) -> true ; (_) -> false end,
            Storage_URL = lists:filter(Storage_URL_F, Head),
            Storage_Token_F = fun ({"X-Storage-Token", _}) -> true ; (_) -> false end,
            Storage_Token = lists:filter(Storage_Token_F, Head),
            {ok,{element(2,lists:nth(1, Storage_URL)), element(2,lists:nth(1, Storage_Token))}};
        _ ->
            couch_log:debug("swift v1: not authenticated: ~p~n", [ReturnCode]),
            {not_authenticated, ReturnCode}
    end.

extractAuthInfo([{_,_}|_] = Obj,[])-> 
    Storage_URL = proplists:get_value(<<"publicURL">>, Obj),
    Storage_Token = proplists:get_value(<<"id">>, Obj),
    Status = true,
    [Storage_URL, Storage_Token, Status];
extractAuthInfo([{[{_,_}|_]}] = Obj, [])->
    [{ObjNext}] = Obj,
    Storage_URL = proplists:get_value(<<"publicURL">>, ObjNext),
    Storage_Token = proplists:get_value(<<"id">>, ObjNext),
    Status = true,
    [Storage_URL, Storage_Token, Status];
extractAuthInfo([{_,_}|_]=Obj,KeyValPath)->
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


