const fs = require('fs');

const code = fs.readFileSync('./components/forecast-map.tsx', 'utf8');

let stack = [];
let lines = code.split('\n');

for (let i = 0; i < lines.length; i++) {
    let line = lines[i];
    let stripped = line.replace(/\/\/.*$/, '').replace(/'.*?'/g, "''").replace(/".*?"/g, '""').replace(/`.*?`/g, '``');
    for (let j = 0; j < stripped.length; j++) {
        let char = stripped[j];
        if (char === '{' || char === '(' || char === '[') {
            stack.push({ char, line: i + 1, col: j });
        } else if (char === '}' || char === ')' || char === ']') {
            if (stack.length === 0) {
                console.log(`Unmatched closing '${char}' at line ${i + 1}, col ${j}`);
                process.exit(1);
            }
            let last = stack.pop();
            let expected = last.char === '{' ? '}' : last.char === '(' ? ')' : ']';
            if (char !== expected) {
                console.log(`Mismatched closing '${char}' at line ${i + 1}, col ${j}. Expected '${expected}' to match '${last.char}' from line ${last.line}`);
                // Print surrounding code context
                console.log("Context:");
                for(let k = Math.max(0, i - 10); k < Math.min(lines.length, i + 10); k++) {
                    console.log(`${k+1}: ${lines[k]}`);
                }
                process.exit(1);
            }
        }
    }
}

if (stack.length > 0) {
    let last = stack[stack.length - 1];
    console.log(`Unmatched opening '${last.char}' at line ${last.line}, col ${last.col}`);
    process.exit(1);
}

console.log('All braces matched!');
