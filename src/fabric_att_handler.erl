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

-export([att_store/2, att_get/2, att_delete/2, dataroot/2, att_store/5, external_store/0]).


%% ====================================================================
%% External API functions. Exposes a generic API that can be used with 
%% other backend drivers
%% ====================================================================

att_store(FileName, Db, ContentLen, MimeType, Req) ->
    DatarootName = dataroot_name(Db),
    couch_log:info("Going to store ~p of length is ~p,"
                        ++ " in the dataroot: ~p ~n",[FileName, ContentLen,
                                                      DatarootName]),
    %TO-DO: No chunk reader. All kept in the memory. Should be fixed.
    Data = recv(Req, ContentLen),
    case get_backend_driver() of
        {ok, BackendDriver} ->
            Headers = [{"Content-Length", ContentLen}],
            case (BackendDriver):put_object(DatarootName, FileName, MimeType,
                                            Headers, Data) of
                {ok, NewUrl} ->
                    case (BackendDriver):head_object(DatarootName, FileName) of
                        {ok, {ObjectSize, EtagMD5}} ->
                            {ok, {unicode:characters_to_binary(NewUrl, unicode, utf8),
                                  ObjectSize, EtagMD5, (BackendDriver):store_id()}};
                        {error, _} ->
                            {error, Data}
                    end;
                {error, _} ->
                    {error, Data}
            end;
        {error, undefined} ->
            {error, Data}
    end.

att_store(Db, #doc{}=Doc) ->
  att_store(Db, [Doc]);
att_store(Db, Docs) when is_list(Docs) ->
    DatarootName = dataroot_name(Db),
    [#doc{atts=Atts0} = Doc | Rest] = Docs,
    if 
        length(Atts0) > 0 ->
            Atts = [att_processor(DatarootName,Att) || Att <- Atts0],
            [Doc#doc{atts = Atts} | Rest];
        true ->
            Docs
    end.

att_get(Db, Att) ->
    DatarootName = dataroot_name(Db),
    [Name,AttLen,AttLenUser] = couch_att:fetch([name, att_len,
                                                att_extstore_size], Att),
    couch_log:info("Going to retrieve ~p. DB length is: ~p, "
                   ++ "stored length: ~p~n", [Name, AttLen, AttLenUser]),
    case get_backend_driver() of
        {ok, BackendDriver} ->
            case (BackendDriver):get_object(DatarootName, Name) of
            {ok, NewData} ->
                NewAtt = couch_att:store([{data, NewData}, {att_len, AttLenUser},
                                          {disk_len, AttLenUser}], Att),
                {ok, NewAtt};
            {error, _} ->
                {error, "Get att ~p~n failed",[Name]}
            end;
        {error, undefined} ->
                {error, "Get att ~p~n failed",[Name]}
    end.

att_delete(Db, Att) ->
    %% Will be called during database compaction.
    %% To-Do.
    DatarootName = dataroot_name(Db),
    [Name] = couch_att:fetch([name],Att),
    couch_log:debug("Delete ~p from ~p", [Name, DatarootName]).

dataroot(create, DbName) ->
    couch_log:info("Create dataroot ~p~n", [DbName]),
    case get_backend_driver() of
        {ok, BackendDriver} ->
            case (BackendDriver):create_dataroot(DbName) of
                {ok, Dataroot} ->
                    couch_log:debug("Dataroot ~p created succesfully ~n", [Dataroot]),
                    {ok, Dataroot};
                {error, _} ->
                    couch_log:debug("Dataroot ~p creation failed ~n", [DbName]),
                    {error, DbName}
            end;
        {error, undefined} ->
           {error, "Create ~p~n failed",[DbName]}
    end;
dataroot(delete, DbName)->
    %% Will be called when database is completely deleted.
    %% To-Do
    couch_log:debug("Delete dataroot ~p~n", [DbName]),
    case get_backend_driver() of
        {ok, BackendDriver} ->
            (BackendDriver):delete_dataroot(DbName);
        {error, undefined} ->
                {error, "Delete ~p~n failed",[DbName]}
    end.

external_store() ->
    config:get_boolean("ext_store", "attachments_offload", false).

%% ====================================================================
%% Internal general functions
%% ====================================================================

recv(#httpd{mochi_req=MochiReq}, Len) ->
    MochiReq:recv(Len).

get_backend_driver() ->
    case config:get("ext_store", "active_store", undefined) of
        "swift" ->
            {ok, fabric_swift_driver };
        undefined ->
            {error, undefined}
    end.

dataroot_name(Db) ->
    Suffix = list_to_binary(mem3:shard_suffix(Db)),
    DbNameSuffix = erlang:iolist_to_binary([fabric:dbname(Db), Suffix]),
    DbNameSuffix.

att_processor(DbName, Att) ->
    BackendDriver = get_backend_driver(),
    [Name, Data, Type, Enc] = couch_att:fetch([name, data, type, encoding], Att),
    couch_log:debug("Att name: ~p, Type: ~p, Encoding: ~p ~n", [Name, Type, Enc]),
    case is_binary(Data) of
        true ->
            case (BackendDriver):put_object(DbName, Name, Type, [], Data) of
                {ok, NewUrl} ->
                    N1 = unicode:characters_to_binary(NewUrl, unicode, utf8),
                    NewAtt = couch_att:store(data, N1, Att),
                    case (BackendDriver):head_object(DbName, Name) of
                        {ok, {ObjectSize,EtagMD5}} ->
                            NewAtt1 = couch_att:store([{att_extstore_size, ObjectSize},
                                                       {att_extstore_id, (BackendDriver):store_id()},
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
