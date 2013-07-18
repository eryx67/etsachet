%%% @author Vladimir G. Sekissov <eryx67@gmail.com>
%%% @copyright (C) 2013, Vladimir G. Sekissov
%%% @doc
%%%
%%% @end
%%% Created : 18 Jul 2013 by Vladimir G. Sekissov <eryx67@gmail.com>

-module(etsachet_gen_config).

-export([generate/2]).

generate(ModName, Config) ->
    Module = erl_syntax:attribute(erl_syntax:atom(module),[erl_syntax:atom(ModName)]),
    ModForm =  erl_syntax:revert(Module),
    ConfigFExp = erl_syntax:arity_qualifier(erl_syntax:atom(config),erl_syntax:integer(0)),
    Export = erl_syntax:attribute(erl_syntax:atom(export), [erl_syntax:list([ConfigFExp])]),
    ExportForm = erl_syntax:revert(Export),
    ConfigBody = Config,
    ConfigFClause = erl_syntax:clause([],[],[erl_syntax:abstract(ConfigBody)]),
    ConfigF = erl_syntax:function(erl_syntax:atom(config),[ConfigFClause]),
    ConfigFForm = erl_syntax:revert(ConfigF),
    Ret = {ok, ModName, Bin} = compile:forms([ModForm, ExportForm, ConfigFForm]),
    {module, ModName} = code:load_binary(ModName, [], Bin),
    Ret.
