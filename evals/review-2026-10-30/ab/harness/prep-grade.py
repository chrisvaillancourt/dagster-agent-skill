import json,re,glob,hashlib,sys
qs={k['id']:k for k in json.load(open('questions.json'))}
def parse(t):
    out={}
    for m in re.finditer(r'^#+\s*Q(\d+)\s*\n(.*?)(?=^#+\s*Q\d+|\Z)', t, re.S|re.M): out[int(m.group(1))]=m.group(2).strip()
    return out
for model in ['opus','sol']:
    ans={a:{} for a in 'TDS'}
    for a in 'TDS':
        for b in range(4):
            try: ans[a].update(parse(open(f'runs/{model}-{a}-{b}/answer.md').read()))
            except FileNotFoundError: pass
    items=[]; mapping={}
    for qid,k in sorted(qs.items()):
        order=sorted('TDS',key=lambda a:hashlib.md5(f'{model}{qid}{a}'.encode()).hexdigest())
        labels=dict(zip(['X','Y','Z'],order)); mapping[qid]=labels
        txt=f"QUESTION:\n{k['q']}\n\nANSWER KEY (verified against Dagster 1.13.28):\n{k['a']}\n\nCOMMON WRONG ANSWER:\n{k['trap']}\n"
        for L,a in labels.items(): txt+=f"\nANSWER {L}:\n{ans[a].get(qid,'(missing)')}\n"
        items.append({'id':qid,'text':txt})
    json.dump(items,open(f'grade-{model}.json','w')); json.dump(mapping,open(f'map-{model}.json','w'))
    print(model,{a:len(ans[a]) for a in 'TDS'})
