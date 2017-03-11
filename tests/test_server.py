def test_imports():
    from neuroarch_component.neuroarch_component import neuroarch_server
    from neuroarch_component.neuroarch_component import user_list
    from neuroarch_component.neuroarch_component import query_processor
    assert True

def test_neuroarch_component():
    from neuroarch_component.neuroarch_component import neuroarch_server
    server = neuroarch_server()
    assert True

def test_neuroarch_component_task_command_restart():
    from neuroarch_component.neuroarch_component import neuroarch_server
    server = neuroarch_server()
    task = {"user":"test","command":{"restart":{}}}
    output = server.recieve_task(task)
    assert output == "None"

def test_neuroarch_component_task_query_multiple_has():
    from neuroarch_component.neuroarch_component import neuroarch_server
    from neuroarch.query import QueryWrapper
    import pandas
    server = neuroarch_server()
    task = {"format":"information","user":"test","query":[
        {"action": {"method": {"query": {"name": "lamina"}}}, "object": {"class": "Neuropil"}},
        {"action": {"method": {"traverse_owns":{"node_class":"Cartridge"}}},"object":{"memory":0}},
        {"action": {"method": {"has":{"name":"home"}}},"object":{"memory":0}},
        {"action": {"method": {"traverse_owns":{"node_class":"Neuron"}}},"object":{"memory":0}}
        ]}

    output = server.recieve_task(task)
    user = server.user_list.user("test")
    p1 = pandas.read_json(output)
    k = p1.index.values[0]
    assert p1.loc[k]["class"] == "Neuron"

def test_neuroarch_component_task_query_single():
    from neuroarch.query import QueryWrapper
    from neuroarch_component.neuroarch_component import neuroarch_server
    import pandas
    server = neuroarch_server()
    task = {"format":"morphology","user":"test","query":[{"action": {"method": {"query": {"name": "lamina"}}}, "object": {"class": "Neuropil"}}]}
    output = server.recieve_task(task)
    user = server.user_list.user("test")
    assert type(output) ==  str
    p1 = pandas.read_json(output)
    k = p1.index.values[0]
    assert p1.loc[k]["name"] == "lamina"

def test_neuroarch_component_task_query_single_query_two_steps():
    from neuroarch_component.neuroarch_component import neuroarch_server
    from neuroarch.models import Neuron
    from neuroarch.query import QueryWrapper
    import pandas
    server = neuroarch_server()
    task = {"format":"morphology","user":"test","query":[{"action": {"method": {"query": {"name": "lamina"}}}, "object": {"class": "Neuropil"}}, {"action": {"method": {"traverse_owns": {"node_class": "Neuron", "max_levels": 2}}}, "object": {"memory": 0}}]}
    output = server.recieve_task(task)
    user = server.user_list.user("test")
    assert type(output) ==  str
    p1 = pandas.read_json(output)
    k = p1.index.values[0]
    assert p1.loc[k]["class"] == "Neuron"

    

def test_neuroarch_component_task_query_multiple():
    from neuroarch_component.neuroarch_component import neuroarch_server
    from neuroarch.models import Neuron
    from neuroarch.query import QueryWrapper
    import pandas
    server = neuroarch_server()
    task_1 = {"format":"morphology","user":"test","query":[{"action": {"method": {"query": {"name": "lamina"}}}, "object": {"class": "Neuropil"}}]}
    task_2 = {"format":"morphology","user":"test", "query":[{"action": {"method": {"traverse_owns": {"node_class": "Neuron", "max_levels": 2}}}, "object": {"state": 0}}]}
    user = server.user_list.user(task_1["user"])
    output_1 = server.process_query(task_1,user)
    output_2 = server.process_query(task_2,user)
    user = server.user_list.user("test")
    
    assert type(output_1) ==  str
    p1 = pandas.read_json(output_1)
    print p1
    k = p1.index.values[0]
    assert p1.loc[k]["name"] == "lamina"

    assert type(output_2) ==  str
    p2 = pandas.read_json(output_2)
    k = p2.index.values[0]
    assert p2.loc[k]["class"] == "Neuron"

def test_neuroarch_component_task_process_morphology():
    import pandas
    from neuroarch_component.neuroarch_component import neuroarch_server
    from neuroarch.models import Neuron
    from neuroarch.query import QueryWrapper
    server = neuroarch_server()
    task = {"format":"morphology","user":"test","query":[{"action": {"method": {"query": {"name": "L1"}}}, "object": {"class": "Neuron"}}]}
    user = server.user_list.user(task["user"])
    output = server.process_query(task,user)
    assert type(output) ==  str
    j = pandas.read_json(output)
    print j.columns
    assert "x" in j.columns

def test_neuroarch_component_task_process_morphology():
    import json
    import pandas
    from neuroarch_component.neuroarch_component import neuroarch_server
    from neuroarch.models import Neuron
    from neuroarch.query import QueryWrapper
    server = neuroarch_server()
    task = {"format":"morphology","user":"test","query":[{"action": {"method": {"query": {"name": "L1"}}}, "object": {"class": "Neuron"}}]}
    user = server.user_list.user(task["user"])
    output = server.process_query(task,user)
    assert type(output) ==  str
    j = pandas.read_json(output)
    j_cols = j.columns
    assert "class" in  j.columns and "name" in  j.columns

def test_neuroarch_component_task_process_op_add_sub():
    import json
    import pandas
    from neuroarch_component.neuroarch_component import neuroarch_server
    from neuroarch.models import Neuron
    from neuroarch.query import QueryWrapper
    server = neuroarch_server()
    task_1 = {"format":"morphology","user":"test","query":[{"action": {"method": {"query": {"name": "L1"}}}, "object": {"class": "Neuron"}}]}
    user = server.user_list.user(task_1["user"])
    output = server.process_query(task_1,user)
    assert type(output) ==  str
    j = pandas.read_json(output)
    task_1_len = len(j.index.values)


    # First Add
    task_2 = {"format":"morphology","user":"test","query":[{"action": {"method": {"query": {"name": "L2"}}}, "object": {"class": "Neuron"}},{"action":{"op":{"__add__":{"state":0}}},"object":{"memory":0}}]}
    user = server.user_list.user(task_2["user"])
    
    output_2 = server.process_query(task_2,user)
    j = pandas.read_json(output_2)
    task_2_len = len(j.index.values)

    #assert len(j.index.values) == task_1_len *2

    # First Subtract
    task_3 = {"format":"morphology","user":"test","query":[{"action": {"method": {"query": {"name": "L2"}}}, "object": {"class": "Neuron"}},{"action":{"op":{"__sub__":{"state":0}}},"object":{"memory":0}}]}
    user = server.user_list.user(task_3["user"])
    
    output_3 = server.process_query(task_3,user)
    j = pandas.read_json(output_3)
    assert len(j.index.values) == task_1_len + task_2_len - task_2_len

def test_neuroarch_component_task_process_op_add():
    import json
    import pandas
    from neuroarch_component.neuroarch_component import neuroarch_server
    from neuroarch.models import Neuron
    from neuroarch.query import QueryWrapper
    server = neuroarch_server()

    task_0 = {"format":"morphology","user":"test","query":[{"action": {"method": {"query": {"name": "L2"}}}, "object": {"class": "Neuron"}}]}
    user = server.user_list.user(task_0["user"])
    output = server.process_query(task_0,user)
    L2_num = len(pandas.read_json(output).index.values)

    task_1 = {"format":"morphology","user":"test","query":[{"action": {"method": {"query": {"name": "L1"}}}, "object": {"class": "Neuron"}}]}
    user = server.user_list.user(task_1["user"])
    output = server.process_query(task_1,user)
    L1_num = len(pandas.read_json(output).index.values)

    assert type(output) ==  str
    task_2 = {"format":"morphology","user":"test","query":[{"action": {"method": {"query": {"name": "L2"}}}, "object": {"class": "Neuron"}},{"action":{"op":{"__add__":{"state":0}}},"object":{"memory":0}}]}
    user = server.user_list.user(task_2["user"])
    
    output = server.process_query(task_2,user)
    j = pandas.read_json(output)
    assert len(j.index.values) == L1_num + L2_num


def test_neuroarch_component_task_process_op_and():
    import json
    import pandas
    from neuroarch_component.neuroarch_component import neuroarch_server
    from neuroarch.models import Neuron
    from neuroarch.query import QueryWrapper
    server = neuroarch_server()
    task_1 = {"format":"morphology","user":"test","query":[{"action": {"method": {"query": {"name": "L1"}}}, "object": {"class": "Neuron"}}]}
    user = server.user_list.user(task_1["user"])
    output = server.process_query(task_1,user)
    assert type(output) ==  str
    task_2 = {"format":"morphology","user":"test","query":[{"action": {"method": {"query": {"name": "L2"}}}, "object": {"class": "Neuron"}},{"action":{"op":{"__and__":{"state":0}}},"object":{"memory":0}}]}
    user = server.user_list.user(task_2["user"])
    output = server.process_query(task_2,user)
    j = pandas.read_json(output)
    assert len(j.index.values) == 0

