const data = JSON.parse(document.getElementById('content').textContent);
const isIndex = Array.isArray(data);
const strings = {
  fr: {home:'← Tous les scénarios', map:'Lire chaque branche : pourquoi → comment → exemple. La carte est en français.', full:'Ouvrir l’image en grand', summary:'Comprendre le scénario', problem:'Problème', solution:'Solution mise en place', result:'Résultat observé', limits:'Point à retenir — portée du test', questions:'Questions de situation', instruction:'Répondre à voix haute avant d’ouvrir la réponse. Chaque question propose une explication et un exemple simple.', example:'Exemple simple', show:'Afficher toutes les réponses', hide:'Masquer les réponses', practice:'Réponse d’entretien : symptôme → preuve → cause → correction → validation → limites.', source:'README du scénario', code:'Voir les fichiers', previous:'Précédent', next:'Suivant', title:'Databricks — carnet de scénarios', intro:'Retrouver le problème, la décision technique et les preuves de chaque exercice. Une carte de raisonnement pour comprendre ; des questions de situation pour s’entraîner.', search:'Chercher un scénario ou une notion', placeholder:'Ex. : checkpoint, versions, watermark…', open:'Lire le scénario →', all:'Tous', routine:'La routine de révision', steps:['Lire le problème et prédire le résultat avant la correction.','Suivre un fichier, une clé ou un événement dans la mind map.','Relier la correction aux lignes du code dans le dossier du scénario.','Comparer les résultats avant/après et rejouer le test quand c’est possible.','Répondre aux questions sans ouvrir la réponse, puis refaire l’exercice en anglais.','Terminer avec la preuve obtenue et les limites du test.'], empty:'Aucun scénario trouvé. Essayer une autre notion ou retirer le filtre.', categories:{ingestion:'Ingestion', cdc:'CDC', replay:'Replay', streaming:'Streaming avec état', performance:'Performance'}},
  en: {home:'← All scenarios', map:'Read each branch: why → how → example. The map is in French.', full:'Open the full-size image', summary:'Understand the scenario', problem:'Problem', solution:'Implemented solution', result:'Observed result', limits:'Key takeaway — test scope', questions:'Situational questions', instruction:'Answer aloud before opening the answer. Each question includes an explanation and a simple example.', example:'Simple example', show:'Show all answers', hide:'Hide answers', practice:'Interview response: symptom → evidence → cause → correction → validation → limits.', source:'Scenario README', code:'Browse the files', previous:'Previous', next:'Next', title:'Databricks — scenario notebook', intro:'Review the problem, technical decision and evidence from each exercise. A reasoning map to understand; situational questions to practise.', search:'Find a scenario or concept', placeholder:'E.g. checkpoint, versions, watermark…', open:'Read the scenario →', all:'All', routine:'Revision routine', steps:['Read the problem and predict the outcome before the fix.','Trace a file, key or event through the mind map.','Connect the fix to the code in the scenario folder.','Compare before/after results and replay the test when possible.','Answer without opening the solution, then practise in English.','Finish with the evidence obtained and the limits of the test.'], empty:'No scenario found. Try another concept or remove the filter.', categories:{ingestion:'Ingestion', cdc:'CDC', replay:'Replay', streaming:'Stateful streaming', performance:'Performance'}}
};
const escape = s => s.replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
const normalized = s => s.normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
let lang = 'fr';
let category = 'all';
try {lang = localStorage.getItem('revision-language') === 'en' ? 'en' : 'fr';} catch {}
const put = (id, value) => {const el=document.getElementById(id); if(el) el.textContent=value;};
function updateAnswersButton(){
  const open=[...document.querySelectorAll('details')].every(d=>d.open);
  put('answers',open?strings[lang].hide:strings[lang].show);
  document.getElementById('answers').setAttribute('aria-expanded',String(open));
}
function renderPage(){
  const t=strings[lang];
  put('home',t.home); put('title',data.title[lang]); put('mapCaption',t.map); put('fullImage',t.full);
  document.getElementById('imageLink').setAttribute('aria-label',t.full);
  put('summaryTitle',t.summary); put('overview',data.summary[lang]);
  for(const field of ['problem','solution','result','limits']){put(field+'Label',t[field]);put(field,data[field][lang]);}
  put('questionsTitle',t.questions);put('instruction',t.instruction);put('practice',t.practice);
  put('source',t.source);put('code',t.code);
  const opened=[...document.querySelectorAll('details[open]')].map(d=>d.id);
  document.getElementById('questions').innerHTML=data.topics.map((topic,i)=>'<details id="q'+(i+1)+'"'+(opened.includes('q'+(i+1))?' open':'')+'><summary><span class="number">'+(i+1)+'</span><span>'+escape(topic[lang][1])+'</span></summary><p>'+escape(topic[lang][2])+'</p><strong class="example-label">'+t.example+'</strong><pre>'+escape(topic[lang][3])+'</pre></details>').join('');
  document.querySelectorAll('details').forEach(d=>d.addEventListener('toggle',updateAnswersButton));
  for(const direction of ['previous','next']){
    const item=data[direction]; if(!item) continue;
    document.getElementById(direction).innerHTML='<span>'+t[direction]+'</span>'+escape(item.title[lang]);
  }
  updateAnswersButton();
}
function renderCards(){
  const t=strings[lang];
  const query=normalized(document.getElementById('search').value.trim());
  const visible=data.filter(s=>(category==='all'||s.category===category)&&normalized(JSON.stringify([s.title,s.summary,s.problem,s.solution,s.slug])).includes(query));
  put('count',lang==='fr'?`${visible.length} scénario${visible.length>1?'s':''} sur ${data.length}`:`${visible.length} of ${data.length} scenarios`);
  document.getElementById('cards').innerHTML=visible.map(s=>{
    const id=String(s.number).padStart(3,'0');
    return '<a class="card" href="scenario_'+id+'.html"><img src="images/scenario_'+id+'.png" alt="" loading="lazy" width="1536" height="1024"><div class="card-body"><div class="card-tag">'+id+' · '+t.categories[s.category]+'</div><h3>'+escape(s.title[lang])+'</h3><p>'+escape(s.summary[lang])+'</p><span class="card-action">'+t.open+'</span></div></a>';
  }).join('');
  document.getElementById('empty').hidden=visible.length>0;put('empty',t.empty);
}
function renderIndex(){
  const t=strings[lang];
  put('title',t.title);put('intro',t.intro);put('searchLabel',t.search);document.getElementById('search').placeholder=t.placeholder;
  put('statScenarios',lang==='fr'?`${data.length} scénarios implémentés`:`${data.length} implemented scenarios`);
  put('statQuestions',lang==='fr'?'96 questions · FR / EN':'96 questions · FR / EN');
  put('statMaps',lang==='fr'?'12 mind maps':'12 mind maps');
  document.getElementById('filters').innerHTML=['all',...Object.keys(t.categories)].map(key=>'<button data-category="'+key+'" aria-pressed="'+(category===key)+'">'+(key==='all'?t.all:t.categories[key])+'</button>').join('');
  document.querySelectorAll('[data-category]').forEach(button=>button.onclick=()=>{category=button.dataset.category;renderIndex();});
  put('routineTitle',t.routine);document.getElementById('routine').innerHTML=t.steps.map(s=>'<li>'+escape(s)+'</li>').join('');
  put('practice',t.practice);renderCards();
}
function render(){
  document.documentElement.lang=lang;
  for(const language of ['fr','en']) document.getElementById(language).setAttribute('aria-pressed',String(lang===language));
  if(isIndex) renderIndex(); else renderPage();
  document.title=isIndex?strings[lang].title:data.title[lang]+' — Databricks';
}
for(const language of ['fr','en']) document.getElementById(language).onclick=()=>{lang=language;try{localStorage.setItem('revision-language',lang);}catch{}render();};
if(isIndex) document.getElementById('search').addEventListener('input',renderCards);
else document.getElementById('answers').onclick=()=>{const items=[...document.querySelectorAll('details')];const open=!items.every(d=>d.open);items.forEach(d=>d.open=open);updateAnswersButton();};
render();
