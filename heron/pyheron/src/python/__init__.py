'''PyHeron's top level library, compatible with StreamParse API'''
__all__ = ['bolt', 'spout', 'stream', 'component', 'topology']

#########################################################################################
### Import modules that topology writers will frequently need to use/implement to     ###
### this top level, so that they can import them from the top level library directly. ###
#########################################################################################

# from heron's common module
from heron.api.src.python.task_hook import ITaskHook
from heron.api.src.python.custom_grouping import ICustomGrouping
from heron.api.src.python.tuple import Tuple
from heron.api.src.python.topology_context import TopologyContext

# Load basic topology modules
from .stream import Stream, Grouping
from .topology import Topology, TopologyBuilder

# Load spout and bolt
from .bolt import Bolt
from .spout import Spout

###### Not yet implemented classes ######
class BatchingBolt(object):
  def __init__(self, *args, **kwargs):
    raise NotImplementedError("BatchingBolt is not yet implemented in PyHeron")

class JavaBolt(object):
  def __init__(self, *args, **kwargs):
    raise NotImplementedError("JavaBolt is not yet implemented in PyHeron")

class JavaSpout(object):
  def __init__(self, *args, **kwargs):
    raise NotImplementedError("JavaSpout is not yet implemented in PyHeron")

class ShellBolt(object):
  def __init__(self, *args, **kwargs):
    raise NotImplementedError("ShellBolt is not yet implemented in PyHeron")

class ShellSpout(object):
  def __init__(self, *args, **kwargs):
    raise NotImplementedError("ShellSpout is not yet implemented in PyHeron")

class StormHandler(object):
  def __init__(self, serializer):
    raise NotImplementedError("StormHandler is not yet implemented in PyHeron")
