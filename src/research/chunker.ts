export type Chunk = { seq: number; text: string; startLine?: number; endLine?: number };

export const chunkText = (text: string, targetTokens = 256): Chunk[] => {
  const lines = text.split(/\r?\n/);
  const chunks: Chunk[] = [];
  let buf: string[] = [];
  let startLine = 1;
  const emit = () => {
    if (!buf.length) return;
    chunks.push({
      seq: chunks.length,
      text: buf.join("\n").trim(),
      startLine,
      endLine: startLine + buf.length - 1,
    });
    startLine += buf.length;
    buf = [];
  };
  for (const line of lines) {
    const words = line.split(/\s+/).filter(Boolean);
    const bufWords = buf.join(" ").split(/\s+/).filter(Boolean);
    if (bufWords.length + words.length > targetTokens && buf.length > 0) {
      emit();
    }
    buf.push(line);
  }
  emit();
  return chunks;
};
