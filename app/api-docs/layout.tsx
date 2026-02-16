export default function ApiDocsLayout({ children }: { children: React.ReactNode }) {
    return (
        <div className="overflow-auto h-screen">
            {children}
        </div>
    )
}
