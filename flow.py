from nodes import (
    GetUserQuery,
    GetDatabaseSchema,
    ConvertToSQL,
    ExecuteSQL,
    HandleError,
    DecideNextAction,
    GenerateInsights
)

class Flow:
    """Flow class to orchestrate node execution"""
    
    def __init__(self, start):
        """
        Initialize a flow with a start node
        
        Args:
            start: The starting node for the flow
        """
        self.start = start
        self.transitions = {}
        self.params = {}
    
    def set_params(self, params):
        """
        Set parameters for the flow
        
        Args:
            params: Dictionary of parameters
        """
        self.params = params
    
    def run(self, shared):
        """
        Run the flow from the start node
        
        Args:
            shared: The shared data store
            
        Returns:
            The final state of the shared store
        """
        current_node = self.start
        
        while current_node:
            # Run the current node
            action = current_node.run(shared)
            
            # Find the next node based on the action
            if action in self.transitions.get(current_node, {}):
                current_node = self.transitions[current_node][action]
            else:
                # No more transitions, end the flow
                break
        
        return shared

# Method to create transitions between nodes
def add_transition(node_from, action, node_to):
    """
    Add a transition between nodes
    
    Args:
        node_from: Source node
        action: Action that triggers the transition
        node_to: Destination node
    """
    if not hasattr(node_from, 'transitions'):
        node_from.transitions = {}
    
    node_from.transitions[action] = node_to

# Define operator overloads for cleaner syntax
def connect_default(node_a, node_b):
    """Connect node_a to node_b with 'default' action"""
    if not hasattr(node_a, 'transitions'):
        node_a.transitions = {}
    
    node_a.transitions["default"] = node_b
    return node_a

def connect_action(node_a, action, node_b):
    """Connect node_a to node_b with specific action"""
    if not hasattr(node_a, 'transitions'):
        node_a.transitions = {}
    
    node_a.transitions[action] = node_b
    return node_a

# Create the NL to SQL conversion flow
def create_nl_to_sql_flow():
    """
    Create and configure the NL to SQL conversion flow
    
    Returns:
        The configured flow
    """
    # Create nodes
    get_query = GetUserQuery()
    get_schema = GetDatabaseSchema()
    convert = ConvertToSQL()
    decide_action = DecideNextAction()
    execute = ExecuteSQL()
    generate_insights = GenerateInsights()
    handle_error = HandleError()
    
    # Create a flow transitions dict
    transitions = {}
    
    # Add nodes to transitions dict
    transitions[get_query] = {"default": get_schema}
    transitions[get_schema] = {"default": convert, "error": handle_error}
    transitions[convert] = {"default": decide_action}
    
    # New decision point based on AI response
    transitions[decide_action] = {
        "execute_sql": execute,
        "ask_question": None  # End flow to return question to user
    }
    
    # After executing SQL, end the flow. Insights will be generated asynchronously after response.
    transitions[execute] = {"default": None, "error": handle_error}
    # transitions[generate_insights] = {"default": None}  # No longer part of synchronous flow
    transitions[handle_error] = {"complete": None}
    
    # Create the flow
    flow = Flow(start=get_query)
    flow.transitions = transitions
    
    return flow

def create_sql_generation_flow():
    """
    Create and configure a flow that only generates SQL without executing it
    
    Returns:
        The configured flow
    """
    # Create nodes
    get_query = GetUserQuery()
    get_schema = GetDatabaseSchema()
    convert = ConvertToSQL()
    decide_action = DecideNextAction()
    handle_error = HandleError()
    
    # Create a flow transitions dict
    transitions = {}
    
    # Add nodes to transitions dict
    transitions[get_query] = {"default": get_schema}
    transitions[get_schema] = {"default": convert, "error": handle_error}
    transitions[convert] = {"default": decide_action}  # Go to decision node
    
    # After decision, the flow ends for this use case
    transitions[decide_action] = {
        "execute_sql": None,
        "ask_question": None
    }
    transitions[handle_error] = {"complete": None}
    
    # Create the flow
    flow = Flow(start=get_query)
    flow.transitions = transitions
    
    return flow