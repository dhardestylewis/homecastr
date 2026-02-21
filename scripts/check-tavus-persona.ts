
import dotenv from "dotenv";
dotenv.config({ path: ".env.local" });

async function checkPersona() {
    const apiKey = process.env.TAVUS_API_KEY;
    const personaId = process.env.TAVUS_PERSONA_ID;

    if (!apiKey || !personaId) {
        console.error("Missing API Key or Persona ID in .env.local");
        return;
    }

    console.log(`Checking Persona: ${personaId}...`);

    try {
        const response = await fetch(`https://tavusapi.com/v2/personas/${personaId}`, {
            headers: {
                "X-Api-Key": apiKey
            }
        });

        if (!response.ok) {
            const err = await response.text();
            console.error(`Error: ${response.status} - ${err}`);
            return;
        }

        const data = await response.json();
        console.log("\n--- Remote Persona Details ---");
        console.log(`Name: ${data.persona_name}`);
        console.log(`System Prompt:\n${data.system_prompt}`);
        console.log("\n--- Tools ---");
        const tools = data.layers?.llm?.tools || [];
        console.log(JSON.stringify(tools, null, 2));
    } catch (e) {
        console.error("Failed to fetch persona:", e);
    }
}

checkPersona();
