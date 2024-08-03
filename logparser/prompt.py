EXTRACT_TEMPLATE="""Extract templates from the given logs. Keep the constants in the logs unchanged and replace the variables with <*>.
If there are no variable (all contents are constants), the template is same as the log.
Every logs and templates are put between tags <START> and <END>.
For example:
{examples}
Based on the above examples and templates, please extract templates from the logs.
Just extract templates from logs. Never output other informations.
Log:
<START>{log}<END>
Template:
"""


VARIABLE_CONSTANT="""Given a log and a specific snippet within the log, you need to determine whether the snippet is a variable or a constant.

You can make the determination based on the following principles:

1. Variables are typically nouns, corresponding to specific objects or entities. If the snippet is a preposition, conjunction, or similar, then answer "No".

2. Variables are typically values dynamically populated into logs (e.g., such as date, time, IP, port, path, boolean values and names), which may vary as more system activities are logged. If the snippet is a variable, then answer "Yes". Please note, it must be the value of a runtime variable, not the name or key of the runtime variable. If the snippet is the name of a runtime variable, then answer "No".

3. Constants typically carry important context and define the interpretation or contextualization of logs, whose value ranges remain unchanged as more system activities are logged (e.g., system states or category labels). If the snippet is a constant, then answer "No".

Do you think the snippet is a variable?
Log: {log}
Snippet: {snippet}
Please provide your response in "Answer" (choose between "Yes" and "No"). 
Remember, do not include any additional content.
Answer:
"""

MERGE_TEMPLATES="""Given two templates extracted from logs, you need to determine whether they are the same template?

You can make the determination based on the following principles:

1. If the templates have different contextual formats, then answer "No".

2. If the differences between them are prepositions, conjunctions, or similar elements, then answer "No".

3. If  any of the differences between them are values dynamically populated into logs (e.g., date, time, IP, port, pathes, and names), which can vary as more system activities are logged, then answer "Yes".

4. If all of the differences between them carry important context and affect the interpretation or contextualization of logs, whose value ranges remain unchanged as more system activities are logged (e.g., system states or category labels), then answer "No".

Do you think the following templates fit all these characteristics?
Template 1: {template1}
Template 2: {template2}
Please provide your response in 'Answer' (Choose between 'Yes' and 'No'). 
Remember, do not include any additional content.
Answer:
"""
