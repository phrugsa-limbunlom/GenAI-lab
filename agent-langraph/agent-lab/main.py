import re

from Agent import Agent

if __name__ == "__main__":
    prompt = """
    You run in a loop of Thought, Action, PAUSE, Observation.
    At the end of the loop you output an Answer
    Use Thought to describe your thoughts about the question you have been asked.
    Use Action to run one of the actions available to you - then return PAUSE.
    Observation will be the result of running those actions.
    
    Your available actions are:
    
    calculate:
    e.g. calculate: 4 * 7 / 3
    Runs a calculation and returns the number - uses Python so be sure to use floating point syntax if necessary
    
    average_dog_weight:
    e.g. average_dog_weight: Collie
    returns average weight of a dog when given the breed
    
    Example session:
    
    Question: How much does a Bulldog weigh?
    Thought: I should look the dogs weight using average_dog_weight
    Action: average_dog_weight: Bulldog
    PAUSE
    
    You will be called again with this:
    
    Observation: A Bulldog weights 51 lbs
    
    You then output:
    
    Answer: A bulldog weights 51 lbs
    """.strip()


    def calculate(what):
        return eval(what)


    def average_dog_weight(name):
        if name in "Scottish Terrier":
            return "Scottish Terriers average 20 lbs"
        elif name in "Border Collie":
            return "a Border Collies average weight is 37 lbs"
        elif name in "Toy Poodle":
            return "a toy poodles average weight is 7 lbs"
        else:
            return "An average dog weights 50 lbs"


    known_actions = {
        "calculate": calculate,
        "average_dog_weight": average_dog_weight
    }

    agent = Agent(prompt)

    # result = agent("How much does a toy poodle weight?")
    # print(result)
    #
    # result = average_dog_weight("Toy Poodle")
    # print(result)
    #
    # next_prompt = average_dog_weight("Toy Poodle")
    # agent(next_prompt)
    # print(agent.messages)
    #
    # agent = Agent(prompt)
    #
    # question = """I have 2 dogs, a border collie and a scottish terrier. \
    # What is their combined weight"""
    #
    # next_prompt = "Observation: {}".format(average_dog_weight("Border Collie"))
    # print(next_prompt)
    # agent(next_prompt)
    # print(agent.messages)
    #
    # next_prompt = "Observation: {}".format(average_dog_weight("Scottish Terrier"))
    # print(next_prompt)
    # agent(next_prompt)
    # print(agent.messages)
    #
    # next_prompt = "Observation: {}".format(eval("37 + 20"))
    # print(next_prompt)
    # agent(next_prompt)
    # print(agent.messages)
    #
    # # loop
    action_re = re.compile('^Action: (\w+): (.*)$')  # python regular expression to selection action


    def query(question, max_turns=5):
        i = 0
        bot = Agent(prompt)
        next_prompt = question
        while i < max_turns:
            i += 1
            result = bot(next_prompt)
            print(result)
            actions = [
                action_re.match(a)
                for a in result.split('\n')
                if action_re.match(a)
            ]
            if actions:
                # There is an action to run
                action, action_input = actions[0].groups()
                if action not in known_actions:
                    raise Exception("Unknown action: {}: {}".format(action, action_input))
                print(" -- running {} {}".format(action, action_input))
                observation = known_actions[action](action_input)
                print("Observation:", observation)
                next_prompt = "Observation: {}".format(observation)
            else:
                return


    question = """I have 2 dogs, a border collie and a scottish terrier. What is their combined weight?"""
    query(question)
