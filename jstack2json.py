import json
import re
import sys
from sys import stdout

date_re = re.compile('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]*$')
jdk_re = re.compile('^Full thread dump')
threadState_re = re.compile('^java.lang.Thread.State:')
threadId_re = re.compile('.*#([0-9]+).*')

output_stack = []

def push_output(name, opening, closing):
    stdout.write(opening)
    output_stack.append({'name': name, 'hasContent': False, 'closingChar': closing})

def output_item(item):
    if output_stack[-1]['hasContent']:
        stdout.write(',')
    else:
        output_stack[-1]['hasContent'] = True
    stdout.write(item)

def pop_output():
    item = output_stack.pop()
    stdout.write(item['closingChar'])

def output_context():
    return output_stack[-1]['name']

push_output('top-level', '[', ']')

for filename in sys.argv[1:]:
    with open(filename, 'r') as file:
        for line_number, line in enumerate(file, 1):
            line = line.strip()
            if date_re.match(line) is not None:
                while len(output_stack) > 1:
                    pop_output()
                output_item('')
                push_output('dump', '{', '}')
                output_item('"date":"{}"'.format(line))
            elif jdk_re.match(line) is not None:
                if output_context() != 'dump':
                    raise Exception('unexpected JDK id on line {}'.format(line_number))
                output_item('"jdk":{}'.format(json.dumps(line[17:-1])))
            elif len(line) == 0:
                if output_context() == 'stack':
                    pop_output()
            elif line[0] == '"':
                if output_context() == 'dump':
                    output_item('"threads":')
                    push_output('threads', '[', ']')
                elif output_context() == 'thread':
                    pop_output()
                else:
                    raise Exception('unexpected thread identifier on line {}'.format(line_number))
                output_item('')
                push_output('thread', '{','}')
                output_item('"header":{}'.format(json.dumps(line)))
                
                closeQuotePos = line[1:].find('"')
                if closeQuotePos != -1:
                    output_item('"name":{}'.format(json.dumps(line[1:closeQuotePos+1])))

                threadId_match = threadId_re.match(line)
                if threadId_match is not None:
                    output_item('"id":{}'.format(threadId_match.group(1)))

                if ' daemon ' in line:
                    output_item('"daemon":true')
                else:
                    output_item('"daemon":false')

                for meta in line.split():
                    eqPos = meta.find('=')
                    if eqPos != -1:
                        key = meta[:eqPos]
                        value = meta[eqPos+1:]
                        if value.endswith('ms'):
                            key = key + '_millis'
                            value = value[:-2]
                        elif value.endswith('s'):
                            key = key + '_secs'
                            value = value[:-1]
                        output_item('{}:{}'.format(json.dumps(key), json.dumps(value)))

            elif threadState_re.match(line) is not None:
                if output_context() != 'thread':
                    raise Exception('unexpected thread state on line {}'.format(line_number))
                output_item('"state":{}'.format(json.dumps(line[24:])))
            elif output_context() == 'thread':
                output_item('"stack":')
                push_output('stack', '[', ']')
                output_item('{}'.format(json.dumps(line)))
            elif output_context() == 'stack':
                output_item('{}'.format(json.dumps(line)))

while len(output_stack) > 0:
    pop_output()