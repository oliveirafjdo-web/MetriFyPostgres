
function sortTable(colIndex) {
    var table = document.getElementById("tabela");
    var switching = true;
    var dir = "desc"; // começa do maior para o menor

    while (switching) {
        switching = false;
        var rows = table.rows;

        for (var i = 1; i < (rows.length - 1); i++) {
            var shouldSwitch = false;
            var x = rows[i].getElementsByTagName("TD")[colIndex];
            var y = rows[i + 1].getElementsByTagName("TD")[colIndex];

            var xVal = parseFloat(x.innerHTML.toString().replace("R$", "").replace(",", "."));
            var yVal = parseFloat(y.innerHTML.toString().replace("R$", "").replace(",", "."));

            if (isNaN(xVal) || isNaN(yVal)) {
                xVal = x.innerHTML.toLowerCase();
                yVal = y.innerHTML.toLowerCase();
            }

            if (dir === "asc") {
                if (xVal > yVal) {
                    shouldSwitch = true;
                    break;
                }
            } else if (dir === "desc") {
                if (xVal < yVal) {
                    shouldSwitch = true;
                    break;
                }
            }
        }

        if (shouldSwitch) {
            rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
            switching = true;
        } else {
            if (dir === "desc") {
                dir = "asc";
                switching = true;
            }
        }
    }
}
