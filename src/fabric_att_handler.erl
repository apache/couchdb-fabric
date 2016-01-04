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

-module(fabric_att_handler).

-include_lib("fabric/include/fabric.hrl").
-include_lib("couch/include/couch_db.hrl").

-export([att_store/2, att/3, container/2, att_store/5, external_store/0]).


%% ====================================================================
%% External API functions. Exposes a generic API that can be used with 
%% other backend drivers
%% ====================================================================

att_store(FileName, Db, ContentLen, MimeType, Req) ->
    ContainerName = container_name(Db),
    couch_log:debug("Going to store ~p of length is ~p," 
                   ++ " ~p in the container: ~n",[FileName, ContentLen,
                                                  ContainerName]),
    %TO-DO: No chunk reader. All kept in the memory. Should be fixed.
    Data = couch_httpd:recv(Req, ContentLen),
    case (get_backend_driver()):put_object(ContainerName, FileName, MimeType,
                                           [], Data) of
        {ok,{"201",NewUrl}} ->
            couch_log:debug("Object ~p created.Response code is 201 ~n",
                            [FileName]),
            case (get_backend_driver()):head_object(ContainerName, FileName) of
                {ok, {ObjectSize, EtagMD5}} ->
                    {ok, {unicode:characters_to_binary(NewUrl, unicode, utf8),
                          ObjectSize, EtagMD5, (get_backend_driver()):store_id()}};
                {error, _} ->
                    {error, Data}
            end;
        {error, _} ->
            {error, Data}
    end.

att_store(Db, #doc{}=Doc) ->
  att_store(Db, [Doc]);
att_store(Db, Docs) when is_list(Docs) ->
    DbName = container_name(Db),
    [#doc{atts=Atts0} = Doc | Rest] = Docs,
    if 
        length(Atts0) > 0 ->
            Atts = [att_processor(DbName,Att) || Att <- Atts0],
            [Doc#doc{atts = Atts} | Rest];
        true ->
            Docs
    end.

att(get, Db, Att) ->
    DbName = container_name(Db),
    [Name,AttLen,AttLenUser] = couch_att:fetch([name, att_len,
                                                att_external_size], Att),
    couch_log:debug("Going to retrieve ~p. DB length is: ~p, "
                   ++ "stored length: ~p~n", [Name, AttLen, AttLenUser]),
    NewData = (get_backend_driver()):get_object(DbName, Name),
    NewAtt = couch_att:store([{data, NewData}, {att_len, AttLenUser},
                              {disk_len, AttLenUser}], Att),
    NewAtt;
att(delete,Db, Att) ->
    DbName = container_name(Db),
    [Name] = couch_att:fetch([name],Att),
    couch_log:debug("Delete ~p from ~p", [Name, DbName]).

container(create,DbName) ->
    couch_log:debug("Create container ~p~n", [DbName]),
    case (get_backend_driver()):create_container(DbName) of
        {ok, {"201", Container}} ->
            couch_log:debug("Container ~p created succesfully ~n", [Container]),
            {ok, Container};
        {error, _} ->
            couch_log:debug("Container ~p creation failed ~n", [DbName]),
            {error, DbName}
    end;
container(get, DbName) ->
    couch_log:debug("Get container ~p~n", [DbName]);
container(delete, DbName)->
    couch_log:debug("Delete container ~p~n", [DbName]),
    (get_backend_driver()):delete_container(DbName).

external_store() ->
    config:get_boolean("ext_store", "attachments_offload", false).

%% ====================================================================
%% Internal general functions
%% ====================================================================

get_backend_driver() ->
    case config:get("ext_store", "active_store") of
        "swift" ->
            fabric_swift_driver;
        undefined ->
            fabric_swift_driver
    end.

container_name(Db) ->
    Suffix = list_to_binary(mem3:shard_suffix(Db)),
    DbNameSuffix = erlang:iolist_to_binary([fabric:dbname(Db), Suffix]),
    DbNameSuffix.

att_processor(DbName, Att) ->
    [Name, Data, Type, Enc] = couch_att:fetch([name, data, type, encoding], Att),
    couch_log:debug("Att name: ~p, Type: ~p, Encoding: ~p ~n", [Name, Type, Enc]),
    case is_binary(Data) of
        true ->
            case (get_backend_driver()):put_object(DbName, Name, Type, [], Data) of
                {ok, {"201", NewUrl}} ->
                    N1 = unicode:characters_to_binary(NewUrl, unicode, utf8),
                    NewAtt = couch_att:store(data, N1, Att),
                    case (get_backend_driver()):head_object(DbName, Name) of 
                        {ok, {ObjectSize,EtagMD5}} ->
                            NewAtt1 = couch_att:store([{att_extstore_size, ObjectSize},
                                                       {att_extstore_id, (get_backend_driver()):store_id()},
                                                       {att_extstore_md5, EtagMD5}], NewAtt),
                            NewAtt1;
                        {error, _} ->
                            Att
                    end;
                {error, _} ->
                    Att
             end;
        _ -> 
            Att
    end.
