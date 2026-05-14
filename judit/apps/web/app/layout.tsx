import type { Metadata } from "next";
import { Inter, Source_Code_Pro } from "next/font/google";

import "./globals.css";

const inter = Inter({
  subsets: ["latin"],
  variable: "--font-sans",
});

const sourceCodePro = Source_Code_Pro({
  subsets: ["latin"],
  variable: "--font-mono",
});

export const metadata: Metadata = {
  title: "Judit Workbench",
  description: "Read-only Judit web workbench demo",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>): JSX.Element {
  return (
    <html lang="en">
      <body className={`${inter.variable} ${sourceCodePro.variable} font-sans`}>{children}</body>
    </html>
  );
}
