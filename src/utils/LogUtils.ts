import fs from "fs";

function fileLog(fileName: string, content: string) {
    if (!fs.existsSync(fileName)) {
        fs.writeFileSync(fileName, content)
    } else {
        fs.appendFileSync(fileName, content);
    }
}

export default fileLog;