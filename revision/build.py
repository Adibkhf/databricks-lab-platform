import json
from html import escape
from pathlib import Path

folder = Path(__file__).resolve().parent
scenarios = json.loads((folder / 'scenarios.json').read_text(encoding='utf-8'))


def document(title, content, data):
    """Construit une page autonome côté données, consultable sans serveur."""
    payload = json.dumps(data, ensure_ascii=False).replace('</', '<\\/')
    return f'''<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)}</title><link rel="stylesheet" href="style.css"><script src="portal.js" defer></script></head>
<body><main>{content}<noscript><p>Activer JavaScript pour consulter les questions bilingues.</p></noscript></main>
<script id="content" type="application/json">{payload}</script></body></html>'''


languages = '<div class="controls" aria-label="Langue / Language"><button id="fr" aria-pressed="true">Français</button><button id="en" aria-pressed="false">English</button></div>'
index_content = f'''
<nav class="topbar" aria-label="Navigation"><a href="../README.md">databricks-lab-platform</a>{languages}</nav>
<div class="eyebrow">EXERCICES · RAISONNEMENT · ENTRETIENS</div>
<h1 id="title">Databricks — carnet de scénarios</h1><p id="intro" class="intro"></p>
<div class="stats"><span id="statScenarios"></span><span id="statQuestions"></span><span id="statMaps"></span></div>
<div class="search"><label for="search" id="searchLabel"></label><input id="search" type="search" autocomplete="off"></div>
<div id="filters" class="filters" aria-label="Thèmes / Topics"></div><p id="count" aria-live="polite"></p>
<div id="cards" class="cards"></div><p id="empty" class="empty" hidden></p>
<section class="routine"><h2 id="routineTitle"></h2><ol id="routine"></ol></section>
<footer><p id="practice"></p></footer>'''
# L'accueil garde seulement les métadonnées utiles à la recherche.
cards = [{key: s[key] for key in ('number', 'slug', 'category', 'title', 'summary', 'problem', 'solution')} for s in scenarios]
(folder / 'index.html').write_text(document('Databricks — carnet de scénarios', index_content, cards), encoding='utf-8')

for position, scenario in enumerate(scenarios):
    number = f"{scenario['number']:03}"
    image = f'images/scenario_{number}.png'
    data = dict(scenario)
    navigation = []
    for direction, offset in [('previous', -1), ('next', 1)]:
        neighbor = position + offset
        if 0 <= neighbor < len(scenarios):
            item = scenarios[neighbor]
            data[direction] = {'title': item['title']}
            navigation.append(f'<a id="{direction}" href="scenario_{item["number"]:03}.html"></a>')
    fields = ''.join(f'<dt id="{key}Label"></dt><dd id="{key}"></dd>' for key in ('problem', 'solution', 'result', 'limits'))
    github = 'https://github.com/Adibkhf/databricks-lab-platform'
    source = f"{github}/blob/main/scenarios/{scenario['slug']}/README.md"
    code = f"{github}/tree/main/scenarios/{scenario['slug']}"
    content = f'''
<nav class="topbar" aria-label="Navigation"><a href="index.html" id="home">← Tous les scénarios</a>{languages}</nav>
<div class="eyebrow">SCÉNARIO {number} · DATABRICKS</div><h1 id="title">{escape(scenario['title']['fr'])}</h1>
<figure id="mindmap"><a href="{image}" id="imageLink" target="_blank" rel="noopener"><img src="{image}" width="1536" height="1024" alt="Mind map : {escape(scenario['title']['fr'], quote=True)}. Raisonnement pourquoi, comment et exemple."></a>
<figcaption><span id="mapCaption"></span><a href="{image}" id="fullImage" target="_blank" rel="noopener"></a></figcaption></figure>
<section class="reading"><h2 id="summaryTitle"></h2><p class="scope" id="overview"></p><dl>{fields}</dl></section>
<section class="questions"><div class="controls"><button id="answers" aria-expanded="false"></button></div>
<h2 id="questionsTitle"></h2><p id="instruction"></p><div id="questions"></div>
<footer><p id="practice"></p><div class="sources"><a id="source" href="{source}" target="_blank" rel="noopener"></a><a id="code" href="{code}" target="_blank" rel="noopener"></a></div>
<nav class="pager" aria-label="Autres scénarios">{''.join(navigation)}</nav></footer></section>'''
    (folder / f'scenario_{number}.html').write_text(document(scenario['title']['fr'], content, data), encoding='utf-8')

print(f'{len(scenarios)} pages et un portail générés.')
