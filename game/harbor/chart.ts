import { gates, voyages, type State } from './game.ts';

type Point = [number, number];
type Pen = CanvasRenderingContext2D;
const W = 840, H = 460;
const waypoints: Point[] = [[57,354],[146,346],[222,288],[292,235],[400,246],[477,203],[542,141],[626,144],[716,117],[790,82]];
function curve(i: number, t: number): Point {
  const a=waypoints[Math.max(0,i-1)], b=waypoints[i], c=waypoints[i+1], d=waypoints[Math.min(waypoints.length-1,i+2)];
  function axis(k: number) {
    return .5*(2*b[k]+(-a[k]+c[k])*t+(2*a[k]-5*b[k]+4*c[k]-d[k])*t*t+(-a[k]+3*b[k]-3*c[k]+d[k])*t*t*t);
  }
  return [axis(0),axis(1)];
}
const route: Point[] = [waypoints[0]], lengths = [0];
for(let i=0;i<waypoints.length-1;i++) for(let n=1;n<=24;n++){
  const p=curve(i,n/24), last=route[route.length-1];
  route.push(p); lengths.push(lengths[lengths.length-1]+Math.hypot(p[0]-last[0],p[1]-last[1]));
}
const total = lengths[lengths.length-1];
function position(f: number): Point {
  const target=Math.min(1,Math.max(0,f))*total;
  const i=Math.max(1,lengths.findIndex(value=>value>=target));
  const a=route[i-1], b=route[i], t=(target-lengths[i-1])/(lengths[i]-lengths[i-1]);
  return [a[0]+t*(b[0]-a[0]),a[1]+t*(b[1]-a[1])];
}
function path(c: Pen, points: Point[], close=false) {
  c.beginPath();points.forEach(([x,y],i)=>i===0?c.moveTo(x,y):c.lineTo(x,y)); if(close)c.closePath();
}
function shape(c:Pen,points:Point[],fill:string) {path(c,points,true);c.fillStyle=fill;c.fill();}
function line(c:Pen,points:Point[],color:string,width=1) {path(c,points);c.strokeStyle=color;c.lineWidth=width;c.stroke();}
function circle(c:Pen,x:number,y:number,r:number,color:string,fill=false) {
  c.beginPath(); c.arc(x,y,r,0,Math.PI*2);
  if(fill){c.fillStyle=color;c.fill();}else{c.strokeStyle=color;c.lineWidth=1.25;c.stroke();}
}
function label(c:Pen,s:string,x:number,y:number,color:string,size=12) {
  c.font='600 '+size+'px ui-monospace, "PingFang SC", monospace'; c.fillStyle=color;c.fillText(s,x,y);
}
function coast(c:Pen,points:Point[]) {
  c.save();c.lineJoin='round';path(c,points,true);
  for(const [width,color] of [[60,'#285a60'],[58,'#12444d'],[38,'#306268'],[36,'#194b50'],[17,'#43746e'],[15,'#265652'],[4,'#839178']] as const){
    c.lineWidth=width;c.strokeStyle=color;c.stroke();
  }
  c.fillStyle='#526957';c.fill();c.clip();
  for(let y=-20;y<H+20;y+=8) for(let x=0;x<W;x+=12){
    const d=Math.sin(x*.32+y*1.9); if(d>.22)line(c,[[x,y],[x+5,y-2]],'#6b8064');
  }
  c.restore();
}
function house(c:Pen,x:number,y:number,s=1) {
  c.save();c.translate(x,y);c.scale(s,s);
  shape(c,[[0,0],[18,-4],[27,1],[9,6]],'#293f3e');
  shape(c,[[0,-4],[16,-8],[16,-2],[0,2]],'#cbb58c');
  shape(c,[[16,-8],[23,-3],[23,1],[16,-2]],'#9c815e');
  shape(c,[[-2,-4],[4,-11],[20,-15],[15,-8]],'#d88364');
  shape(c,[[15,-8],[20,-15],[25,-8],[23,-3]],'#9b5949');
  c.fillStyle='#405451';c.fillRect(3,-3,3,4);c.restore();
}
function lightTower(c:Pen,x:number,y:number) {
  c.save();c.translate(x,y);
  shape(c,[[-9,0],[10,-3],[37,8],[23,12]],'#304740');
  shape(c,[[-9,0],[9,0],[5,-42],[-4,-42]],'#eee3ba');
  shape(c,[[-8,-12],[8,-12],[7,-24],[-6,-24]],'#c3604a');
  shape(c,[[4,-41],[9,0],[5,0],[1,-41]],'#364c4950');
  c.fillStyle='#233d43';c.fillRect(-9,-43,18,3);c.fillRect(-6,-54,12,11);
  c.fillStyle='#ffe9a1';c.fillRect(-4,-52,3,7);c.fillRect(1,-52,3,7);
  shape(c,[[-9,-54],[0,-61],[9,-54]],'#bd684c');line(c,[[0,-62],[0,-67]],'#e5c58c');
  c.restore();
}
function baseChart():HTMLCanvasElement {
  const canvas=document.createElement('canvas');canvas.width=W*2;canvas.height=H*2;
  const c=canvas.getContext('2d')!;c.scale(2,2);
  const sea=c.createLinearGradient(0,0,W,H);sea.addColorStop(0,'#0c303f');sea.addColorStop(.5,'#123e49');sea.addColorStop(1,'#0d3340');c.fillStyle=sea;c.fillRect(0,0,W,H);
  for(let x=0;x<W;x+=40)line(c,[[x,0],[x,H]],'#8cbdba13');
  for(let y=0;y<H;y+=40)line(c,[[0,y],[W,y]],'#8cbdba13');
  let seed=19;const draw=()=>((seed=(seed*1664525+1013904223)>>>0)/4294967296);
  for(let i=0;i<540;i++){
    const x=draw()*W,y=draw()*H,l=2+draw()*8;line(c,[[x,y],[x+l*.4,y-1],[x+l,y]],'#89b9b52b',.8);
  }
  coast(c,[[-50,-30],[264,-30],[241,9],[211,15],[205,30],[176,35],[163,52],[136,57],[128,73],[98,70],[81,84],[52,87],[42,103],[13,101],[-20,121]]);
  coast(c,[[880,-25],[759,-25],[744,0],[752,17],[738,28],[750,40],[776,44],[770,56],[802,62],[804,74],[831,87],[857,81]]);
  coast(c,[[-30,324],[5,327],[16,343],[26,344],[32,357],[48,368],[45,383],[65,392],[63,406],[81,413],[103,442],[122,470],[-30,480]]);
  coast(c,[[434,502],[456,461],[486,450],[499,429],[522,429],[545,411],[565,413],[572,394],[597,390],[610,369],[628,366],[641,350],[659,354],[677,346],[690,355],[711,345],[731,352],[744,341],[772,339],[795,319],[819,321],[856,306],[876,495]]);
  coast(c,[[317,100],[329,89],[343,93],[356,84],[370,88],[365,100],[348,109],[334,108],[322,115],[312,111]]);
  // Docks, bollards and their planks follow the coast.
  for(const [x,y] of [[35,369],[785,59]]){
    c.save();c.translate(x,y);c.rotate(-.2);c.fillStyle='#483f34';c.fillRect(0,0,49,7);c.fillStyle='#ab916b';c.fillRect(0,-2,49,6);
    for(let j=2;j<49;j+=5)line(c,[[j,-2],[j,4]],'#796b55');c.restore();
  }
  for(const [x,y,s] of [[38,52,1.1],[66,40,.9],[85,26,1],[108,35,.85],[16,400,.95],[33,420,1.15],[750,24,.7],[784,31,.8],[729,384,.95],[754,375,.8]])house(c,x,y,s);
  lightTower(c,657,365);
  // The lighthouse beam is anchored to its lantern, not the ship.
  c.save();c.translate(657,316);
  const beam=c.createRadialGradient(0,0,4,0,0,275);beam.addColorStop(0,'#ffda8d70');beam.addColorStop(1,'#ffda8d00');
  shape(c,[[0,0],[-267,-48],[-228,-149]],'#ffffff00');c.fillStyle=beam;c.fill();c.restore();
  label(c,'西湾邮埠',40,324,'#bbd1c4',12);label(c,'月牙灯塔',626,393,'#e2d6ae',12);label(c,'东港',770,109,'#cbd7c7',12);
  c.save();c.translate(61,170);line(c,[[0,24],[0,-22]],'#a0b5a6');line(c,[[-16,0],[16,0]],'#a0b5a6');
  shape(c,[[0,-21],[-4,0],[0,-4]],'#e1c98f');shape(c,[[0,-21],[4,0],[0,-4]],'#659189');label(c,'N',-4,-28,'#dacfaa',11);c.restore();
  c.save();c.setLineDash([3,5]);line(c,route,'#bdd4bd90',1.6);c.restore();
  return canvas;
}
function ship(c:Pen,x:number,y:number,angle:number) {
  c.save();c.translate(x,y);c.rotate(angle);
  line(c,[[-24,-4],[-34,-6],[-46,-5]],'#cee6d19a',1.7);line(c,[[-24,3],[-40,5],[-53,4]],'#cee6d17a',1.5);
  shape(c,[[2,3],[-10,11],[-27,9],[-27,-3],[-9,-5]],'#092d34aa');
  shape(c,[[0,0],[-9,7],[-23,7],[-27,3],[-27,-5],[-22,-8],[-9,-7]],'#f5ddac');
  shape(c,[[-2,0],[-10,5],[-22,5],[-24,2],[-24,-4],[-20,-6],[-10,-5]],'#c47748');
  c.fillStyle='#f0c98a';c.fillRect(-21,-4,10,8);c.fillStyle='#405863';c.fillRect(-15,-3,3,6);c.fillStyle='#854a37';c.fillRect(-22,-3,4,6);
  circle(c,-2,0,1.5,'#fff1be',true);c.restore();
}
export function chart(canvas: HTMLCanvasElement) {
  const base=baseChart(), c=canvas.getContext('2d')!;
  canvas.width=W*2;canvas.height=H*2;c.scale(2,2);
  return (s:State)=>{
    c.drawImage(base,0,0,W,H);
    const v=voyages[s.mode], f=s.elapsed/v.duration;
    gates.forEach((gate,i)=>{
      const [x,y]=position(gate), active=i===s.marks.length, mark=s.marks[i];
      const color=mark==='miss'?'#ed9276':mark?'#f5cf76':active?'#99f2c9':'#99bdaf';
      const r=active ? v.window/v.duration*total : 9;
      circle(c,x,y,r,active?'#85f9bc26':'#183e42',true);
      circle(c,x,y,r,color);
      if(active){c.save();c.setLineDash([3,5]);circle(c,x,y,r+5,'#8feacb80');c.restore();circle(c,x,y,Math.max(3,v.perfect/v.duration*total),'#efcd83');}
      if(mark==='miss'){line(c,[[x-3,y-3],[x+3,y+3]],color);line(c,[[x+3,y-3],[x-3,y+3]],color);}
      else circle(c,x,y,mark?4:2,color,true);
      label(c,'0'+(i+1),x-7,y-(active?r:9)-10,color,11);
    });
    const p=position(f), ahead=position(Math.min(1,f+.003)), behind=position(Math.max(0,f-.003));
    if(s.elapsed-s.flashAt<430){const t=(s.elapsed-s.flashAt)/430;c.save();c.globalAlpha=1-t;circle(c,p[0],p[1],6+35*t,'#ffdb81');c.restore();}
    ship(c,...p,Math.atan2(ahead[1]-behind[1],ahead[0]-behind[0]));
  };
}
