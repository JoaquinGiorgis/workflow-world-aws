import { getWritable } from 'workflow';

interface Visitor {
  name: string;
  arrivedAt: string;
}

/** Step 1: looks the visitor up (here it only stamps the time). */
async function lookUpVisitor(name: string): Promise<Visitor> {
  'use step';
  return { name, arrivedAt: new Date().toISOString() };
}

/**
 * Step 2: streams a greeting word by word to the run's default stream, and
 * returns it whole.
 */
async function streamGreeting(visitor: Visitor): Promise<string> {
  'use step';
  const words = ['Hello', `${visitor.name},`, 'this', 'came', 'through', 'AWS'];
  const writer = getWritable<string>().getWriter();
  for (const word of words) {
    await writer.write(`${word} `);
  }
  await writer.close();
  return words.join(' ');
}

export async function greeting(name: string): Promise<string> {
  'use workflow';
  const visitor = await lookUpVisitor(name);
  return await streamGreeting(visitor);
}
