## Algo 1 Simplified:

```
all_completing = True
for c in w:
    all_completing = False if not c.completing else all_completing
    G = growth_score(c)
    if G < alpha and not c.completing:
        c.completing = True if c.watching else False
        c.watching = not c.watching
    elif G >= alpha:
        c.watching = c.completing = False
        
if all_completing:
    for c in w:
        c.cpu_lim = 1 / len(w)
        
else:
    for c in w:
        if not c.watching:
            growth_ratio = growth_score(c) / sum(growth_score(c) for c in w)  # compute on fly??
            c.cpu_lim *= 1 - growth_ratio if c.completing else 1 + growth_ratio      
```