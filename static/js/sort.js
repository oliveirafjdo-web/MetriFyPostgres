function sortTable(n){
  var table=document.getElementById("tabela"), rows, switching=true, i, x, y, shouldSwitch;
  while(switching){
    switching=false;
    rows=table.rows;
    for(i=1;i<rows.length-1;i++){
      shouldSwitch=false;
      x=rows[i].getElementsByTagName("TD")[n];
      y=rows[i+1].getElementsByTagName("TD")[n];
      if(parseFloat(x.innerHTML) < parseFloat(y.innerHTML)){
        shouldSwitch=true;
        break;
      }
    }
    if(shouldSwitch){
      rows[i].parentNode.insertBefore(rows[i+1], rows[i]);
      switching=true;
    }
  }
}