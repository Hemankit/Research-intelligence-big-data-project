/**
 * api/mock.js
 * Drop-in mock data for frontend dev without a running backend.
 * Set VITE_USE_MOCK=true in .env.local to enable.
 */

export const MOCK_STATS = {
  total_papers: 2_431_800,
  full_text_papers: 847_200,
  citation_edges: 12_100_000,
  topic_clusters: 312,
  papers_today: 847,
  last_ingestion: new Date(Date.now() - 2 * 3600 * 1000).toISOString(),
}

export const MOCK_TRENDS = {
  labels: ['Q1 21','Q2 21','Q3 21','Q4 21','Q1 22','Q2 22','Q3 22','Q4 22',
           'Q1 23','Q2 23','Q3 23','Q4 23','Q1 24','Q2 24','Q3 24','Q4 24'],
  series: [
    { name: 'Transformers',    color: '#3b82f6', data: [18,22,27,31,38,44,49,54,61,68,74,79,85,90,94,98] },
    { name: 'Diffusion',       color: '#10b981', data: [2,3,4,6,9,15,22,34,45,57,63,71,78,83,88,91] },
    { name: 'GNNs',            color: '#f59e0b', data: [14,16,18,20,22,24,27,28,30,32,33,35,36,37,38,39] },
    { name: 'RLHF/Alignment',  color: '#a78bfa', data: [1,1,2,2,3,4,7,12,21,36,52,65,74,80,85,88] },
    { name: 'RAG',             color: '#2dd4bf', data: [0,1,1,2,3,5,9,14,22,33,44,54,62,68,72,75] },
  ],
}

export const MOCK_METHOD_ADOPTION = [
  { method: 'LoRA',            count_2023: 2100, count_2024: 7200, growth: 243 },
  { method: 'Flash Attention', count_2023: 1800, count_2024: 5400, growth: 200 },
  { method: 'PEFT',            count_2023: 1400, count_2024: 4100, growth: 193 },
  { method: 'RAG',             count_2023: 900,  count_2024: 3800, growth: 322 },
  { method: 'DPO',             count_2023: 600,  count_2024: 3200, growth: 433 },
  { method: 'Chain-of-Thought',count_2023: 1200, count_2024: 2900, growth: 142 },
  { method: 'RLHF',            count_2023: 800,  count_2024: 2600, growth: 225 },
  { method: 'ControlNet',      count_2023: 700,  count_2024: 1800, growth: 157 },
]

export const MOCK_INFLUENTIAL_PAPERS = [
  { id:'p1', title:'Attention Is All You Need', authors:'Vaswani et al.', year:2017, venue:'NeurIPS', category:'Transformers', pagerank:98, citations:92100, abstract:'We propose a new network architecture based solely on attention mechanisms.' },
  { id:'p2', title:'LoRA: Low-Rank Adaptation of Large Language Models', authors:'Hu et al.', year:2021, venue:'ICLR', category:'PEFT', pagerank:91, citations:14200, abstract:'We propose LoRA which freezes pretrained model weights and injects trainable rank decomposition matrices.' },
  { id:'p3', title:'Denoising Diffusion Probabilistic Models', authors:'Ho et al.', year:2020, venue:'NeurIPS', category:'Diffusion', pagerank:89, citations:19800, abstract:'We present high quality image synthesis results using diffusion probabilistic models.' },
  { id:'p4', title:'Training Language Models to Follow Instructions with Human Feedback', authors:'Ouyang et al.', year:2022, venue:'NeurIPS', category:'RLHF', pagerank:86, citations:8700, abstract:'We fine-tune GPT-3 to follow a wide variety of written instructions.' },
  { id:'p5', title:'Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks', authors:'Lewis et al.', year:2020, venue:'NeurIPS', category:'RAG', pagerank:82, citations:7400, abstract:'We explore RAG models which combine parametric and non-parametric memory.' },
  { id:'p6', title:'FlashAttention: Fast and Memory-Efficient Exact Attention with IO-Awareness', authors:'Dao et al.', year:2022, venue:'NeurIPS', category:'Efficiency', pagerank:79, citations:5600, abstract:'We propose an IO-aware exact attention algorithm using tiling.' },
  { id:'p7', title:'An Image Is Worth 16x16 Words: Transformers for Image Recognition at Scale', authors:'Dosovitskiy et al.', year:2020, venue:'ICLR', category:'Vision', pagerank:77, citations:28400, abstract:'We show that a pure transformer can perform very well on image recognition.' },
  { id:'p8', title:'Direct Preference Optimization: Your Language Model is Secretly a Reward Model', authors:'Rafailov et al.', year:2023, venue:'NeurIPS', category:'Alignment', pagerank:74, citations:3200, abstract:'We introduce DPO, a stable and computationally lightweight alternative to RLHF.' },
]

export const MOCK_TRENDING = [
  { name:'LoRA fine-tuning',        pct:94, delta:'+12%',  color:'#3b82f6' },
  { name:'Chain-of-Thought prompting',pct:87,delta:'+8%',  color:'#10b981' },
  { name:'Diffusion guidance',      pct:81, delta:'+19%',  color:'#a78bfa' },
  { name:'RLHF / DPO alignment',    pct:74, delta:'+31%',  color:'#2dd4bf' },
  { name:'Flash Attention v2',      pct:68, delta:'+5%',   color:'#f59e0b' },
  { name:'Retrieval-Augmented Gen', pct:61, delta:'+22%',  color:'#f87171' },
  { name:'Vision Transformers',     pct:55, delta:'+3%',   color:'#3b82f6' },
]

export const MOCK_ENTITIES = {
  methods:  ['Attention','LoRA','PEFT','DPO','UMAP','HDBSCAN','BERTopic','GraphX','PageRank','spaCy','RLHF','RAG','Flash Attn','SFT'],
  datasets: ['ImageNet','COCO','Wikipedia','BookCorpus','C4','The Pile','OpenWebText','HumanEval','GSM8K','MMLU','BooksCorpus','CC-News'],
  tasks:    ['Text Classification','NER','Summarization','Image Captioning','QA','Code Generation','Dialogue','Translation'],
}

export const MOCK_PIPELINE_STATUS = [
  { label:'arXiv ingestion',      status:'ok',      last_run:'2h ago',   records:847  },
  { label:'S2ORC fetch',          status:'ok',      last_run:'6h ago',   records:2100 },
  { label:'NER (spaCy + HF)',     status:'running', last_run:'running',  records:null },
  { label:'BERTopic clustering',  status:'running', last_run:'running',  records:null },
  { label:'Spark aggregations',   status:'queued',  last_run:'12h ago',  records:null },
  { label:'Elasticsearch index',  status:'queued',  last_run:'12h ago',  records:null },
  { label:'Hive batch writes',    status:'ok',      last_run:'12h ago',  records:18400},
]

export const MOCK_LANDSCAPE = (() => {
  const clusters = [
    { id:0, name:'Transformers',    cx:0.25, cy:0.35, color:'#3b82f6', count:180 },
    { id:1, name:'Diffusion',       cx:0.65, cy:0.25, color:'#10b981', count:120 },
    { id:2, name:'GNNs',            cx:0.15, cy:0.70, color:'#f59e0b', count:80  },
    { id:3, name:'RLHF/Alignment',  cx:0.55, cy:0.65, color:'#a78bfa', count:95  },
    { id:4, name:'Computer Vision', cx:0.80, cy:0.72, color:'#2dd4bf', count:70  },
    { id:5, name:'NLP / NER',       cx:0.40, cy:0.50, color:'#f87171', count:60  },
  ]
  const rng = (s) => { let x = Math.sin(s) * 10000; return x - Math.floor(x) }
  const points = []
  clusters.forEach((cl, ci) => {
    for (let i = 0; i < cl.count; i++) {
      const angle = rng(ci * 1000 + i) * Math.PI * 2
      const dist  = rng(ci * 500 + i * 7) * 0.12
      points.push({
        x: cl.cx + Math.cos(angle) * dist,
        y: cl.cy + Math.sin(angle) * dist,
        cluster_id: cl.id,
        pagerank: rng(i * 13 + ci),
        color: cl.color,
      })
    }
  })
  return { clusters, points }
})()
