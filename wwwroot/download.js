// wwwroot/download.js
// Descarga mediante Blob (mejor para archivos grandes que data:URI)
window.downloadFileFromBytes = (fileName, base64) => {
    try {
        const byteChars = atob(base64);
        const byteNumbers = new Array(byteChars.length);
        for (let i = 0; i < byteChars.length; i++) {
            byteNumbers[i] = byteChars.charCodeAt(i);
        }
        const byteArray = new Uint8Array(byteNumbers);
        const blob = new Blob(
            [byteArray],
            { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" }
        );

        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = fileName || "archivo.xlsx";
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
    } catch (e) {
        console.error("downloadFileFromBytes error:", e);
        alert("No se pudo iniciar la descarga.");
    }
};
