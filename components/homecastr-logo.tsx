import Image from "next/image"

interface HomecastrLogoProps {
    className?: string
    size?: number
}

export function HomecastrLogo({ className = "", size = 32 }: HomecastrLogoProps) {
    return (
        <Image
            src="/homecastr-logo-cropped.png"
            alt="Homecastr"
            width={size}
            height={size}
            className={className}
            style={{ objectFit: "contain" }}
        />
    )
}
